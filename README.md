# Hiring Pipeline Webhook

AI-scored hiring pipeline for Orange Global Services' **Business Development Lead** role. A candidate's journey is 5 stages — questionnaire, systems test, sales simulation, marketing/outreach, Bitcoin task — each scored by Claude against prompts stored in Notion.

Notion is the system of record for candidates. A Railway-hosted FastAPI webhook watches the Candidate Applications database, scores new Stage 1 applications in real time, and merges Stage 2–5 task submissions into the correct candidate row. Stages 2–5 are still scored in batch via a CLI (`run.py`).

## Why the webhook exists

Notion forms can only **create** rows — they can't update an existing candidate's row. That means every time a candidate submits a task via a Notion form, a NEW row appears in the Candidate Applications database with only the submitted payload (no CV, no demographics). Without intervention, those orphan rows:

1. Pollute the database with duplicates of the same candidate.
2. Trigger Stage 1 hard filters (missing CV → auto-reject), destroying the submission signal.
3. Hide the actual submission data on a "Rejected" row that the per-stage scorer never scans.

The webhook solves this by detecting orphan submissions, matching them back to the original candidate row, and merging in the payload — while enforcing rules that prevent candidates from gaming their scores via re-submission.

## High-level architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Notion: Candidate Applications database                             │
│                                                                      │
│  Stage 1 form ──creates──▶ new row (full application)                │
│  Stage 2 form ──creates──▶ orphan row (just task files + email)      │
│  Stage 3 form ──creates──▶ orphan row (just text submission + email) │
│  Stage 4 form ──creates──▶ orphan row                                │
│  Stage 5 form ──creates──▶ orphan row                                │
│                                                                      │
│  Notion automation: "Page added" ─webhook─▶ Railway                  │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Railway: FastAPI server (server.py → pipeline.process_single_stage1)│
│                                                                      │
│  1. Is this page already archived?         → return                  │
│  2. Does it look like a Stage 2-5 orphan?  → merge + archive orphan  │
│  3. Otherwise, it's a Stage 1 application  → hard filters + Claude   │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│  CLI (run.py) — batch Stage 2-5 scoring                              │
│                                                                      │
│  python run.py --role head_of_sales stage2  (vision: screenshots)    │
│  python run.py --role head_of_sales stage3  (text evaluation)        │
│  python run.py --role head_of_sales stage4  (text evaluation)        │
│  python run.py --role head_of_sales stage5  (Bitcoin verification)   │
│  python run.py --role head_of_sales rank    (composite ranking)      │
└──────────────────────────────────────────────────────────────────────┘
```

## The 5 stages

| Stage | What's measured | Scoring | Pass |
|---|---|---|---|
| 1 — Application | Structured questionnaire + writing sample | Claude text, `/100` | 60+ |
| 2 — Systems Competency | Notion pipeline, Google Sheets, Slides, AI email (screenshots) | Claude vision, `/20` | 12+ |
| 3 — Sales Process | Executive sales simulation (text) | Claude text, `/35` | 22+ |
| 4 — Marketing Outreach | Cold outreach artefacts (text) | Claude text, `/25` | 15+ |
| 5 — Bitcoin Task | On-chain + Lightning payment with screenshots | Automated verification, `/6` | 4+ |

Thresholds and composite weights live in [`config/head_of_sales.yaml`](config/head_of_sales.yaml).

## Stage 2–5 submission auto-merge (the important bit)

Each stage form writes to the same Candidate Applications database. Full Name and Email are **required** on every form, which gives the webhook two signals to reunite the orphan row with the candidate's real record.

### Detection

[`pipeline.py`](pipeline.py) defines `STAGE_SUBMISSION_SPECS` — a table of which file and text properties belong to each stage, plus optional post-merge text-shaping hooks:

```python
STAGE_SUBMISSION_SPECS = [
    {
        "label": "Stage 2",
        "score_prop": "Stage 2 Score",
        "stage_task": "Stage 2 Task",
        "file_props": [...],   # screenshots + Upload your task
        "text_props": [...],   # email-draft text fields + Stage 2 Submission
    },
    {
        "label": "Stage 3",
        "file_props": ["Upload your task"],
        "text_props": ["Stage 3 Submission"],
        "extracted_text_target": "Stage 3 Submission",  # extract DOCX/PDF text into this prop
    },
    {
        "label": "Stage 4",
        "file_props": [],
        "text_props": [<the 9 concept questions>],
        "concat_text_target": "Stage 4 Submission",     # concat the 9 fields into this prop
    },
    {"label": "Stage 5", ...},
]
```

`_detect_stage_submission(candidate, props, current_stage=None)` returns the matching spec if the incoming page has **no Stage 1 data** (no CV, writing test, age, experience) and **at least one** of the spec's fields populated. If so, the webhook branches into the merge path **before** Stage 1 hard filters run — so a legitimate Stage 2 submission is never rejected for missing a CV.

**Disambiguation by current Stage.** Stage 2 and Stage 3 forms both write to `Upload your task`, so an orphan with only that field could belong to either. The webhook calls `_detect_stage_submission` twice: first for a provisional spec (used to find the original candidate), then again with the original's current `Stage` value as a hint. The hint causes the detector to prefer the spec whose `stage_task` matches the candidate's actual progression — Stage 2 form for a candidate at "Stage 2 Task", Stage 3 form for a candidate at "Stage 3 Task". The disambiguation is logged as `[disambiguated Stage X → Stage Y]`.

**Text shaping at merge time.** Two optional spec keys reshape the orphan's payload into the property the per-stage scorer reads:

- `extracted_text_target` — Stage 3's form uploads a DOCX or PDF. The merger downloads the file, runs PDF/DOCX text extraction (`pypdf` / `python-docx`), and writes the result into the named property. `run_stage3` then sees the submission as plain text in `Stage 3 Submission` and scores it normally. Extraction is best-effort; failures log `WARN` but do not block the merge.
- `concat_text_target` — Stage 4's form has 9 separate concept text fields. The merger concatenates them (with `## <prop name>` headers between sections) into the named property so `run_stage4` can score the submission as one blob.

### Matching (who does this submission belong to?)

`_find_original_candidate(name, email, db_id, exclude)` uses **name as the primary key, email as the disambiguator**:

1. **Exact normalised name match** — trim, lowercase, collapse whitespace. One hit → match. Multiple hits → email breaks the tie. If that's still ambiguous, prefer the row with `AI Score Stage 1` set.
2. **No name match** → fall back to email. Handles candidates who typed only a first name, or a minor spelling variation.
3. **Neither resolves** → log `[Stage N unmatched]` and leave the orphan for manual review.

Normalisation explicitly tolerates trailing whitespace (`"Benzile Makhanya "` in the DB vs `"Benzile Makhanya"` typed in the form) which would otherwise break exact-match filters.

### Merging

`_merge_stage_submission(orphan, original, spec)`:

1. Downloads each file from the orphan's signed S3 URL.
2. Re-uploads it via Notion's `file_uploads` API (signed URLs expire; re-upload makes the files permanent on the target page).
3. Copies text properties across.
4. If the spec has `concat_text_target`, concatenates `text_props` into that single property.
5. If the spec has `extracted_text_target`, downloads the file(s) in `file_props`, extracts PDF/DOCX text, and writes it into that property.
6. Archives the orphan row.

### Re-submission policy

| Candidate's current Stage | Submits form for... | Result |
|---|---|---|
| Applied / Stage 1 Review | Stage 2 | Normal first-time merge |
| Stage 2 Task | **Stage 2 again** | **Last-wins** — overwrite files/text, clear `Stage 2 Score` + AI writeup, Stage stays at `Stage 2 Task` so re-scoring picks them up |
| Stage 3 Task | **Stage 2** | **BLOCKED** — files/text discarded, orphan archived, prior score retained (game-prevention) |
| Stage 4+ / Hired | Stage 2 or 3 | **BLOCKED** |
| Stage 3 Task | Stage 3 again | Last-wins (same-stage re-submission) |
| Rejected | anything | Not blocked — treated as unknown ordering, manual review |

The ordering is defined by `STAGE_ORDER` in [`pipeline.py`](pipeline.py). `_is_past_stage(props, spec)` returns whether the candidate has progressed beyond the stage the orphan belongs to.

### Submission timestamps

Every merge records **when** the candidate hit submit on the Notion form, not when the scorer ran, so Ricki can see who's been slow or fast through the pipeline.

- **Stage 1 Submitted At** — set from the application page's own `created_time` inside `process_single_stage1`, after the stage-submission detection branch so it's only written for real Stage 1 applications.
- **Stage 2 / 3 / 4 / 5 Submitted At** — set from the orphan row's `created_time` inside `_merge_stage_submission`, alongside the files/text patch. Each spec in `STAGE_SUBMISSION_SPECS` carries a `submitted_at_prop` naming the target property.

Blocked re-submissions (past-stage game-prevention) do NOT update the timestamp — the earlier legit submission's timestamp is preserved, matching the "their old score just remains as it was" policy. Mid-stage re-submissions (last-wins) DO update it to the newer submission's moment.

### Deadline extensions

Candidates have 14 days from Stage 1 submission to get through all remaining stages. (The window was 7 days through 2026-04-28; extended to 14 to give candidates more breathing room — `config/head_of_sales.yaml` is the source of truth.) If a candidate requests more time, Ricki grants it by setting the **Extended Deadline** date column on their row in the Candidate Applications database. There's no CLI command and no script — the property is the entire UI.

When `Extended Deadline` is set:

- The `Days Left` formula counts down to that date instead of the default 14-day window. Note that Notion's `dateBetween(..., "days")` returns 24-hour chunks elapsed, so a deadline set mid-day can display one less than a calendar-day subtraction might suggest. The actual expiry behaviour (below) handles this correctly.
- `run_timeout_check` in [`pipeline.py`](pipeline.py) computes the warning and expiry moments relative to (Extended Deadline + 24 hours) so an end-of-day override honours the full day — a Friday-2026-04-24 override expires at Saturday 00:00 UTC, not Friday 00:00.
- The warning fires 2 days before expiry (same lead-time as the default behaviour), and Email Action is set to `Timeout Warning` / `Timeout Expired` exactly as without an override.

Leaving `Extended Deadline` empty reverts the candidate to the default behaviour. Clearing it after granting one also reverts immediately.

### Log lines to grep in Railway

```
[Stage N submission detected] page=... name=... email=...
  Merged into <original_id> (fields: [...])

[Stage N re-submission] replaced prior values on <id> for: [...]
[Stage N re-submission] cleared score + AI writeup; reverted Stage to 'Stage N Task'

[disambiguated Stage X → Stage Y] candidate is at 'Stage Y Task'

[Stage N re-submission BLOCKED] candidate is at 'Stage M Task' (past 'Stage N Task');
  retaining prior score and data. Orphan archived.

[Stage N unmatched] ... no matching candidate found — left in place for manual review

  TIMEOUT EXPIRED (extended): <name> deadline was YYYY-MM-DD, in Stage N Task
  TIMEOUT WARNING (extended): <name> deadline is YYYY-MM-DD, in Stage N Task
  OK (extended to YYYY-MM-DD): <name> in Stage N Task
```

## Directory layout

```
automation/
├── server.py                       # FastAPI webhook (Railway entrypoint)
├── pipeline.py                     # Core orchestration — stages 1-5 + auto-merge
├── notion_client.py                # Notion API client + file-transfer helpers
├── evaluator.py                    # Claude text + vision evaluation wrappers
├── bitcoin_verifier.py             # Stage 5 on-chain + Lightning verification
├── run.py                          # CLI entrypoint (batch stage scoring, ranking)
├── config/
│   └── head_of_sales.yaml          # Role config: thresholds, weights, hard filters
├── test_stage_submission_merge.py  # Synthetic tests for merge logic (no Notion calls)
├── Procfile                        # Railway: `web: uvicorn server:app ...`
├── requirements.txt
└── README.md                       # (this file)
```

**Notion IDs** (referenced from code/config):
- Candidate Applications database: `336eddaa-d74c-8188-b616-c32aa4da025e`
- AI Prompts page: `336eddaa-d74c-814e-90f6-db7f3f5d98d6`
- Interview Candidates database: `33eeddaa-d74c-81e5-ad77-e4861d37dc1c`

## Running locally

### Setup

```bash
cd automation
pip install -r requirements.txt
```

Create `automation/.env`:

```
ANTHROPIC_API_KEY=sk-ant-...
NOTION_API_KEY=ntn_...
WEBHOOK_SECRET=<any random string>
ROLE=head_of_sales
```

### CLI (batch scoring)

```bash
# Score all candidates currently at each stage
python run.py --role head_of_sales stage1   # NB: stage 1 is now real-time via webhook
python run.py --role head_of_sales stage2
python run.py --role head_of_sales stage3
python run.py --role head_of_sales stage4
python run.py --role head_of_sales stage5

# Generate composite ranking across all passers
python run.py --role head_of_sales rank

# Run the timeout check (warn at 5 days, reject at 7 — respects Extended Deadline overrides)
python run.py --role head_of_sales timeout
```

### Automation schedule (what runs when)

| Trigger | What fires | Where |
|---|---|---|
| Candidate submits Stage 1 form → Notion "Page added" automation | `process_single_stage1` → hard filters + AI score | Railway webhook (real-time) |
| Candidate submits a Stage 2/3/4/5 form → Notion "Page added" | `_detect_stage_submission` → `_merge_stage_submission` merges the orphan into the candidate's original row, then `_score_single_stage` runs the matching per-stage scorer **inline** so the candidate sees their result within seconds | Railway webhook (real-time) |
| Every 15 minutes (UTC) | `run_stage2`, `run_stage3`, `run_stage4`, `run_stage5`, `run_timeout_check`, `run_health_check` — safety-net retry for any submission whose inline scoring failed, the 14-day timeout sweep, and the stuck-submission detector | GitHub Actions scheduled workflow ([`.github/workflows/cron-scoring.yml`](.github/workflows/cron-scoring.yml)) |
| Push to `main` | `railway up --service hiring-pipeline-webhook` deploys the webhook | GitHub Actions ([`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)) |

The cron is a safety net, not the primary path. GitHub Actions scheduled runs are unreliable in practice — we've observed 40-min to 3-hour gaps despite the 15-min cron — so per-stage scoring runs **inline on the webhook** the moment a submission is merged. If inline scoring throws, the webhook still acks the merge and the next cron tick picks the candidate up; `run_stageN` is idempotent (skips anyone already scored), so re-runs are cheap.

**⚠️ The timeout check now runs automatically.** Before the cron, it only ran when invoked manually, so timeouts could sit for days. Now: a candidate past 14 days (or past their `Extended Deadline`) is auto-rejected within 15 minutes of expiry. If you want to grant a last-minute extension, set `Extended Deadline` on their row BEFORE the 15-minute mark.

### Stuck-submission detector (`run_health_check`)

This runs as the last cron step every 15 min. It scans every candidate row and looks for the symptoms of a stuck submission, regardless of which underlying bug caused it:

- **Orphans** — un-merged form submissions still sitting in the DB → re-runs `process_single_stage1`.
- **Rejected with unscored submission** — submission timestamp set, score still empty → flags for manual review (we don't auto-revive a rejected candidate).
- **Stage N Task with stale unscored submission** — submitted >5 min ago, no score → calls the per-stage scorer once (idempotent). If still unscored after the retry, flags.
- **Broken progression** — at Stage N Task with no Stage N-1 score, and no manual override note in AI Reasoning → flags.

Anything we can't auto-fix lands on the `Pipeline Issue` rich-text column on the Candidate Applications database. Build a Notion view filtered on `Pipeline Issue is not empty` to see them at a glance — the flag self-clears once the issue resolves.

Run on demand: `python run.py --role head_of_sales health`

### Webhook server (local dev)

```bash
uvicorn server:app --reload --port 8000
```

Then POST a mock Notion "Page added" payload:

```bash
curl -X POST http://localhost:8000/webhook/stage1 \
  -H "Content-Type: application/json" \
  -H "x-webhook-secret: $WEBHOOK_SECRET" \
  -d '{"id": "<notion-page-id>"}'
```

Health check: `GET /health`.

### Tests

Pure-Python synthetic tests for the stage-submission logic (no Notion calls):

```bash
python -X utf8 test_stage_submission_merge.py
```

Covers:
- Name normalisation (trailing whitespace, case, internal double-spaces)
- Stage detection (Stage 1 application vs Stage 2/3/5 orphans)
- Matching (exact, trailing-space, first-name-only-with-email, ambiguous-name-with-email, case-insensitive, archived exclusion)
- `_is_past_stage` / game-prevention (Stage 3 Task candidate re-submits Stage 2 → blocked)

## Deployment

Hosted on Railway, auto-deploys from `main` on push to GitHub.

- Repo: [RickiOrange/hiring-pipeline-webhook](https://github.com/RickiOrange/hiring-pipeline-webhook)
- Production URL: `https://hiring-pipeline-webhook-production.up.railway.app`
- Endpoints: `POST /webhook/stage1`, `GET /health`

Railway environment variables mirror `.env`:

- `ANTHROPIC_API_KEY`
- `NOTION_API_KEY`
- `WEBHOOK_SECRET`
- `ROLE` (default `head_of_sales`)

**Notion automation** (configured in Notion, not in this repo): "Page added" trigger on the Candidate Applications database → POST to `<railway-url>/webhook/stage1` with the `x-webhook-secret` header.

## Extending to a new role or stage

### New role

1. Add `config/<role>.yaml` following the head_of_sales schema.
2. Add prompts to a new Notion AI Prompts page and reference its ID in the config.
3. Run with `--role <role>`.

### New stage

1. Add properties to the Candidate Applications database for the stage's files/text and score.
2. Add a Notion form for the new stage's task submissions (require Full Name + Email).
3. Add a new entry to `STAGE_SUBMISSION_SPECS` in [`pipeline.py`](pipeline.py) with its `file_props`, `text_props`, `score_prop`, `stage_task`.
4. Add the new Stage's select value to `STAGE_ORDER` (in the right position).
5. Implement a `run_stageN(config)` function and register it in `run.py`.

## Troubleshooting

**"Candidate submitted but isn't showing up for scoring"**
- Check Railway logs for `[Stage N submission detected]` on the submission timestamp. If present, look for `Merged into <id>` — the original row should now have the files.
- If you see `[Stage N unmatched]`, the form's Name + Email didn't resolve to an existing candidate. Check that the candidate's original row has their actual name spelled the same way, or that they used the same email.
- If you see `[Stage N re-submission BLOCKED]`, the candidate had already progressed past that stage. Their old score is retained — this is game-prevention.

**"A candidate's Stage 2 Score suddenly disappeared"**
- They re-submitted Stage 2 while still at `Stage 2 Task`. Policy is last-wins: old files + score cleared, Stage reverted to `Stage 2 Task`. Run `python run.py ... stage2` to re-score.

**"Stage 1 hard filter rejected a real candidate"**
- A genuine Stage 1 application should have CV + age + experience. If one of those is missing, the hard filters in `_check_hard_filters` fire. The stage-submission detector ONLY triggers when ALL of those are absent AND a stage-specific payload is present — so it can't accidentally rescue a broken Stage 1 application.

**Re-uploading files failed mid-merge**
- Logs will show `WARN: failed to transfer file <name>: <error>`. The orphan is NOT archived in this case (we keep it around so the files can be retried). Re-fire the webhook manually by POSTing to `/webhook/stage1` with the orphan's page ID once the root cause is fixed.

## Design decisions worth knowing

1. **Submission detection runs BEFORE hard filters.** A Stage 2 orphan has no CV by design; hard-filtering it would wrongly reject a legitimate candidate. This invariant is load-bearing — do not reorder the checks in `process_single_stage1`.
2. **File re-upload, not URL reference.** The signed S3 URLs on Notion file properties expire in ~1 hour. The merge re-uploads files via Notion's `file_uploads` API so they persist on the candidate's row indefinitely.
3. **Name-primary, email-disambiguator matching.** Ricki's rule: people rarely misspell their own name, so full name takes priority. Email is only consulted when name alone is ambiguous or missing. This is captured in `_find_original_candidate`.
4. **Last-wins on mid-stage re-submission, blocked once past stage.** Candidates can correct a bad Stage 2 while still at Stage 2 — but once they've moved to Stage 3, they can't retroactively improve their Stage 2. The dividing line is `_is_past_stage`.
5. **Orphan rows are always archived.** Even a blocked or empty merge archives the orphan so the database doesn't fill up with duplicates.
