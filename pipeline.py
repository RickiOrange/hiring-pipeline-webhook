"""Generic hiring pipeline orchestration — 5 stages + final interview."""

import os
import time

import yaml
from dotenv import load_dotenv

from notion_client import (
    get_candidates,
    get_candidate_data,
    get_code_blocks,
    get_page,
    update_candidate,
    advance_candidate,
    reject_candidate,
    transfer_file_to_notion,
    archive_page,
    patch_page_properties,
    _request,
    _make_rich_text_blocks,
)
from evaluator import evaluate_candidate, evaluate_ranking, evaluate_with_images, fetch_cv_content
from bitcoin_verifier import (
    verify_onchain_transaction,
    verify_lightning_payment,
    match_onchain_by_amount,
    match_lightning_by_amount,
    TARGET_BTC_ADDRESS,
)

load_dotenv(override=True)


INTERVIEW_DB_ID = "33eeddaa-d74c-81e5-ad77-e4861d37dc1c"


def set_email_action(page_id: str, action: str):
    """Set Email Action property to trigger a Notion email automation."""
    update_candidate(page_id, {"Email Action": action})


def add_to_interview_database(candidate: dict, page: dict, stage5_score: int | None = None):
    """Add a candidate who passed Stage 5 to the Interview Candidates database.

    Pulls all stage scores from the original candidate page and creates
    a new entry in the interview database for manual scoring.
    """
    from notion_client import _request

    props = page.get("properties", {})

    # Extract scores from the candidate applications database
    s1 = props.get("AI Score Stage 1", {}).get("number")
    s2 = props.get("Stage 2 Score", {}).get("number")
    s3 = props.get("Stage 3 Score", {}).get("number")
    s4 = props.get("Stage 4 Score", {}).get("number")
    # Use passed-in score (fresh) since page data may be stale
    s5 = stage5_score if stage5_score is not None else props.get("Stage 5 Score", {}).get("number")

    interview_props = {
        "Candidate Name": {"title": [{"text": {"content": candidate.get("full_name", "Unknown")}}]},
        "Email": {"email": candidate.get("email")},
        "LinkedIn": {"url": candidate.get("linkedin")},
        "Interview Status": {"select": {"name": "Pending"}},
        "Source Page": {"url": f"https://www.notion.so/{candidate['page_id'].replace('-', '')}"},
    }

    # Add scores (skip None values)
    if s1 is not None:
        interview_props["Stage 1 Score"] = {"number": s1}
    if s2 is not None:
        interview_props["Stage 2 Score"] = {"number": s2}
    if s3 is not None:
        interview_props["Stage 3 Score"] = {"number": s3}
    if s4 is not None:
        interview_props["Stage 4 Score"] = {"number": s4}
    if s5 is not None:
        interview_props["Stage 5 Score"] = {"number": s5}

    resp = _request("POST", "/pages", {
        "parent": {"database_id": INTERVIEW_DB_ID},
        "properties": interview_props,
    })
    print(f"  Added to Interview Candidates database: {resp.get('id', 'error')}")
    return resp


def load_config(role: str) -> dict:
    """Load role configuration from YAML file."""
    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config")
    config_path = os.path.join(config_dir, f"{role}.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_prompts(config: dict) -> list[str]:
    """Read AI prompts from the Notion prompts page."""
    prompts_page_id = config["notion_prompts_page_id"]
    return get_code_blocks(prompts_page_id)


# --- Stage 1: Structured questionnaire + writing test ---

AGE_RANGES_ORDERED = ["Under 25", "25-30", "31-35", "36-40", "41-45", "46-50", "Over 50"]
EXP_RANGES_ORDERED = ["0-2", "3-5", "5-7", "8-10", "10+"]


def _check_hard_filters(candidate: dict, config: dict) -> str | None:
    """Check hard disqualifiers. Returns rejection reason or None if passed."""
    filters = config.get("hard_filters", {})

    # University degree required
    if filters.get("require_degree") and candidate.get("university_degree") != "Yes":
        return "Hard filter: No university degree"

    # Age range check — blank = reject (candidate must provide age)
    max_age = filters.get("max_age_range", "41-45")
    age_range = candidate.get("age_range")
    if not age_range:
        return "Hard filter: Age range not provided"
    max_idx = AGE_RANGES_ORDERED.index(max_age) if max_age in AGE_RANGES_ORDERED else 4
    curr_idx = AGE_RANGES_ORDERED.index(age_range) if age_range in AGE_RANGES_ORDERED else -1
    if curr_idx > max_idx:
        return f"Hard filter: Age range {age_range} exceeds maximum {max_age}"

    # Minimum experience — blank = reject (candidate must provide experience)
    min_exp = filters.get("min_experience", "3-5")
    exp_range = candidate.get("years_sales_range")
    if not exp_range:
        return "Hard filter: Sales experience not provided"
    min_idx = EXP_RANGES_ORDERED.index(min_exp) if min_exp in EXP_RANGES_ORDERED else 1
    curr_idx = EXP_RANGES_ORDERED.index(exp_range) if exp_range in EXP_RANGES_ORDERED else -1
    if curr_idx < min_idx:
        return f"Hard filter: Experience {exp_range} below minimum {min_exp}"

    # Executive sales or closing experience required
    if filters.get("require_executive_or_closing"):
        has_exec = candidate.get("executive_sales") == "Yes"
        has_closing = candidate.get("closed_deals_over_r1m") == "Yes"
        if not has_exec and not has_closing:
            return "Hard filter: No executive sales experience and no deals over R1M"

    return None


# Stage 2+ submission forms each create a NEW row in the Candidate Applications
# database instead of updating the candidate's existing row. The detector and
# merger below recognise those orphan rows by stage and reunite them with the
# original candidate record. Extend this list when a new stage form is added.
STAGE_SUBMISSION_SPECS = [
    {
        "label": "Stage 2",
        "file_props": [
            "Notion task screenshots",
            "Spreadsheet task screenshots",
            "Presentation task screenshots (each slide)",
            "Upload your task",
        ],
        "text_props": [
            "A.I. email draft (1/3) - prompts",
            "A.I. email draft (2/3) - non edited version",
            "A.I. email draft (3/3) - edited version",
            "Stage 2 Submission",
        ],
    },
    {
        "label": "Stage 3",
        "file_props": [],
        "text_props": ["Stage 3 Submission"],
    },
    {
        "label": "Stage 4",
        "file_props": [],
        "text_props": ["Stage 4 Submission"],
    },
    {
        "label": "Stage 5",
        "file_props": [
            "Stage 5 BTC Screenshot",
            "On-chain transaction screenshot",
            "Stage 5 Lightning Screenshot",
            "Lightning payment screenshot",
        ],
        "text_props": [
            "Stage 5 BTC Transaction ID",
            "On-chain transaction ID",
            "Stage 5 Lightning Payment Hash",
            "Lightning transaction proof of payment",
            "How many Satoshis did you send on-chain?",
            "How many Satoshis did you send via Lightning?",
        ],
    },
]


def _has_stage1_data(candidate: dict) -> bool:
    """True if the page carries real Stage 1 application data (CV, writing test,
    age, or experience). Orphan submission pages lack all of these."""
    return bool(
        candidate.get("cv_upload")
        or candidate.get("writing_test")
        or candidate.get("age_range")
        or candidate.get("years_sales_range")
    )


def _has_payload_for_spec(page_props: dict, spec: dict) -> bool:
    """True if the page has at least one populated field named in spec."""
    for prop_name in spec["file_props"]:
        if (page_props.get(prop_name) or {}).get("files"):
            return True
    for prop_name in spec["text_props"]:
        rt = (page_props.get(prop_name) or {}).get("rich_text") or []
        if "".join(t.get("plain_text", "") for t in rt).strip():
            return True
    return False


def _detect_stage_submission(candidate: dict, page_props: dict) -> dict | None:
    """If this page looks like an orphan submission from a stage form, return
    the matching spec. Otherwise None.

    A page is a stage-N submission orphan iff:
      - it has no Stage 1 data (no CV / writing / age / experience), AND
      - at least one field in the stage's file_props or text_props is populated.

    If multiple stages match (unlikely but possible — a form accidentally
    populates fields across stages), the LATEST stage wins so the candidate's
    most-recent submission is honoured.
    """
    if _has_stage1_data(candidate):
        return None
    matches = [s for s in STAGE_SUBMISSION_SPECS if _has_payload_for_spec(page_props, s)]
    if not matches:
        return None
    # Take the latest-stage spec (by list order — specs are ordered Stage 2→5)
    return matches[-1]


def _normalize_name(s: str | None) -> str:
    """Lowercase, strip, and collapse internal whitespace. Used for name
    comparison so trailing spaces and double-spaces don't break matching."""
    if not s:
        return ""
    return " ".join(s.split()).lower()


def _find_original_candidate(name: str, email: str | None, db_id: str,
                             exclude_page_id: str) -> dict | None:
    """Locate the candidate's existing record in the Candidate Applications
    database, using full name as the primary key and email as a tiebreaker.

    Matching order (per Ricki: full name takes priority; email disambiguates):
      1. Exact normalised full-name match → if exactly one non-archived row,
         that's the match. If multiple, disambiguate by email; if email doesn't
         resolve, prefer the row with an AI Score Stage 1 (the real Stage 1
         record) to avoid matching a prior orphan.
      2. No full-name match → match by email. If exactly one row has this
         email, use it (covers cases where the candidate typed only their
         first name, or a minor spelling variation).
      3. Otherwise None — log [unmatched] for manual review.
    """
    all_rows = get_candidates(db_id)
    candidates = [
        r for r in all_rows
        if r["id"] != exclude_page_id and not r.get("archived") and not r.get("in_trash")
    ]

    target_name = _normalize_name(name)
    target_email = (email or "").strip().lower() or None

    # --- 1. Exact normalised full-name match
    name_matches = []
    for c in candidates:
        cd = get_candidate_data(c)
        if _normalize_name(cd.get("full_name")) == target_name and target_name:
            name_matches.append((c, cd))

    if len(name_matches) == 1:
        return name_matches[0][0]

    if len(name_matches) > 1:
        # Disambiguate by email
        if target_email:
            email_hits = [(c, cd) for (c, cd) in name_matches
                          if (cd.get("email") or "").strip().lower() == target_email]
            if len(email_hits) == 1:
                return email_hits[0][0]
            if len(email_hits) > 1:
                # Prefer the one with a real Stage 1 score
                with_score = [(c, cd) for (c, cd) in email_hits
                              if (c.get("properties", {}).get("AI Score Stage 1", {}) or {}).get("number") is not None]
                if with_score:
                    return with_score[0][0]
                return email_hits[0][0]
        # No email or email didn't help → prefer row with Stage 1 score
        with_score = [(c, cd) for (c, cd) in name_matches
                      if (c.get("properties", {}).get("AI Score Stage 1", {}) or {}).get("number") is not None]
        if with_score:
            return with_score[0][0]
        return None  # ambiguous — don't guess

    # --- 2. No name match → fall back to email
    if target_email:
        email_hits = []
        for c in candidates:
            cd = get_candidate_data(c)
            if (cd.get("email") or "").strip().lower() == target_email:
                email_hits.append((c, cd))
        if len(email_hits) == 1:
            return email_hits[0][0]
        if len(email_hits) > 1:
            with_score = [(c, cd) for (c, cd) in email_hits
                          if (c.get("properties", {}).get("AI Score Stage 1", {}) or {}).get("number") is not None]
            if with_score:
                return with_score[0][0]

    return None


def _merge_stage_submission(orphan_page: dict, original_page: dict, spec: dict) -> dict:
    """Copy payload fields from the orphan into the original candidate page,
    then archive the orphan.

    First-wins policy: if the original already has data in a given property,
    DO NOT overwrite it. Log a warning instead. The orphan is still archived
    so the DB doesn't fill up with duplicates.

    Files are re-uploaded via Notion's file_uploads API so they persist on the
    target page (signed S3 URLs from the source would otherwise expire).
    """
    orphan_id = orphan_page["id"]
    original_id = original_page["id"]
    orphan_props = orphan_page.get("properties", {})
    original_props = original_page.get("properties", {})
    label = spec["label"]

    patch_props: dict = {}
    skipped_props: list[str] = []

    # --- File properties
    for prop_name in spec["file_props"]:
        src_prop = orphan_props.get(prop_name) or {}
        src_files = src_prop.get("files", [])
        if not src_files:
            continue
        # First-wins: skip if original already populated
        dest_existing = (original_props.get(prop_name) or {}).get("files") or []
        if dest_existing:
            skipped_props.append(prop_name)
            continue
        new_entries = []
        for f in src_files:
            ftype = f.get("type")
            if ftype == "file":
                url = f.get("file", {}).get("url")
            elif ftype == "external":
                url = f.get("external", {}).get("url")
            else:
                url = None
            fname = f.get("name") or "file"
            if not url:
                continue
            try:
                new_entries.append(transfer_file_to_notion(url, fname))
            except Exception as e:
                print(f"  WARN: failed to transfer file {fname}: {e}")
        if new_entries:
            patch_props[prop_name] = {"files": new_entries}

    # --- Text properties
    for prop_name in spec["text_props"]:
        src_prop = orphan_props.get(prop_name) or {}
        text = "".join(t.get("plain_text", "") for t in src_prop.get("rich_text", []))
        if not text.strip():
            continue
        dest_rt = (original_props.get(prop_name) or {}).get("rich_text") or []
        dest_text = "".join(t.get("plain_text", "") for t in dest_rt).strip()
        if dest_text:
            skipped_props.append(prop_name)
            continue
        patch_props[prop_name] = {"rich_text": _make_rich_text_blocks(text)}

    if skipped_props:
        print(f"  [{label} re-submission] kept existing values on {original_id} for: {skipped_props}")

    if patch_props:
        patch_page_properties(original_id, patch_props)

    # Archive the orphan either way — a re-submission should not leave a dangling row
    archive_page(orphan_id)

    return {
        "merged": bool(patch_props),
        "orphan_id": orphan_id,
        "original_id": original_id,
        "stage_label": label,
        "fields_merged": list(patch_props.keys()),
        "fields_skipped": skipped_props,
    }


def process_single_stage1(page_id: str, config: dict) -> dict:
    """Process a single candidate through Stage 1 (hard filters + AI evaluation).

    Called by the webhook server for real-time processing, and by run_stage1()
    for batch processing. Returns a result dict with the outcome.

    Special-case: if the incoming page looks like a Stage 2 task submission
    (from the task submission form, which unfortunately creates a new DB row
    instead of updating the candidate's existing row), merge the submission
    into the original candidate and skip Stage 1 processing.
    """
    thresholds = config["thresholds"]
    stages = config["stages"]
    db_id = config["notion_database_id"]

    # Fetch the single page
    page = get_page(page_id)
    candidate = get_candidate_data(page)
    name = candidate["full_name"]
    email = candidate.get("email")

    # --- Idempotency: if Notion fires the webhook twice or we're asked to
    # re-process an already-archived page, do nothing.
    if page.get("archived") or page.get("is_archived") or page.get("in_trash"):
        return {"page_id": page_id, "name": name, "decision": "Already archived",
                "score": None, "reasoning": "Page is archived; skipping"}

    # --- Stage 2+ submission auto-merge ---
    # Applied BEFORE the idempotency-by-score check and BEFORE hard filters, so
    # Stage 1 disqualifiers (missing CV, missing age, etc.) do not wrongly reject
    # stage submissions that legitimately carry only task payloads.
    spec = _detect_stage_submission(candidate, page.get("properties", {}))
    if spec:
        label = spec["label"]
        print(f"[{label} submission detected] page={page_id} name={name!r} email={email!r}")
        original = _find_original_candidate(name, email, db_id, exclude_page_id=page_id)
        if not original:
            msg = (f"{label} submission for name={name!r} email={email!r} but no matching "
                   f"candidate found — left in place for manual review")
            print(f"  [{label} unmatched] {msg}")
            return {"page_id": page_id, "name": name, "decision": f"{label} unmatched",
                    "score": None, "reasoning": msg}
        merge_result = _merge_stage_submission(page, original, spec)
        if merge_result.get("merged"):
            print(f"  Merged into {merge_result['original_id']} "
                  f"(fields: {merge_result['fields_merged']})")
            reasoning = f"Merged {label} submission into {merge_result['original_id']}; orphan archived"
            if merge_result.get("fields_skipped"):
                reasoning += f" (skipped fields already populated: {merge_result['fields_skipped']})"
            return {"page_id": page_id, "name": name, "decision": f"{label} merged",
                    "score": None, "reasoning": reasoning}
        else:
            # No new fields written (all conflicts), but orphan was archived
            return {"page_id": page_id, "name": name, "decision": f"{label} duplicate",
                    "score": None,
                    "reasoning": (f"{label} re-submission — all fields already populated on "
                                  f"{merge_result['original_id']}; orphan archived")}

    # Idempotency: skip if already scored
    existing_score = page.get("properties", {}).get("AI Score Stage 1", {}).get("number")
    if existing_score is not None:
        return {"page_id": page_id, "name": name, "decision": "Already processed",
                "score": existing_score, "reasoning": "Skipped — already has Stage 1 score"}

    # Ensure Stage is set to Applied (Notion forms leave it blank)
    current_stage = page.get("properties", {}).get("Stage", {}).get("select")
    if not current_stage:
        update_candidate(page_id, {"Stage": stages["applied"]})

    # Hard filter check BEFORE AI call
    rejection = _check_hard_filters(candidate, config)
    if rejection:
        reject_candidate(page_id, rejection)
        update_candidate(page_id, {
            "AI Score Stage 1": 0,
            "AI Decision Stage 1": "Fail",
            "AI Reasoning": rejection,
            "Red Flags": rejection,
        })
        set_email_action(page_id, "Failed")
        return {"page_id": page_id, "name": name, "decision": "Hard Filter",
                "score": 0, "reasoning": rejection}

    # Load prompts
    prompts = get_prompts(config)
    if not prompts:
        return {"page_id": page_id, "name": name, "decision": "Error",
                "score": None, "reasoning": "No prompts found on the AI Prompts page"}

    stage1_prompt = "\n".join(prompts[:2]) if len(prompts) >= 2 else prompts[0]

    # Fetch CV if available
    cv_urls = candidate.get("cv_upload") or []
    cv_content = fetch_cv_content(cv_urls[0]) if cv_urls else ""

    # Run AI evaluation
    result = evaluate_candidate(candidate, stage1_prompt, cv_content=cv_content)

    score = result.get("score", 0)
    reasoning = result.get("reasoning", "")
    strengths = result.get("strengths", [])
    weaknesses = result.get("weaknesses", [])
    red_flags = result.get("red_flags", [])

    decision = "Pass" if score >= thresholds["stage1_pass"] else "Fail"

    # Update Notion
    update_candidate(page_id, {
        "AI Score Stage 1": score,
        "AI Decision Stage 1": decision,
        "AI Reasoning": reasoning,
        "Strengths": strengths,
        "Weaknesses": weaknesses,
        "Red Flags": red_flags,
    })

    if decision == "Pass":
        advance_candidate(page_id, stages["stage2_task"])
        set_email_action(page_id, "Passed Stage 1")
    else:
        reject_candidate(page_id, reasoning)
        set_email_action(page_id, "Failed")

    return {"page_id": page_id, "name": name, "decision": decision,
            "score": score, "reasoning": reasoning}


def run_stage1(config: dict) -> dict:
    """Screen all 'Applied' candidates through hard filters + AI evaluation."""
    db_id = config["notion_database_id"]
    stages = config["stages"]

    # Get candidates in "Applied" stage AND candidates with no stage set
    # (Notion forms don't auto-set Stage, so new submissions have empty Stage)
    candidates = get_candidates(db_id, stage=stages["applied"])

    from notion_client import _request as _notion_request
    no_stage = _notion_request("POST", f"/databases/{db_id}/query", {
        "filter": {"property": "Stage", "select": {"is_empty": True}},
        "page_size": 100,
    })
    no_stage_pages = no_stage.get("results", [])
    if no_stage_pages:
        # Set them to Applied first, then include them
        for page in no_stage_pages:
            update_candidate(page["id"], {"Stage": stages["applied"]})
        candidates.extend(no_stage_pages)
        print(f"Found {len(no_stage_pages)} new form submission(s) with no stage set — moved to Applied.")

    if not candidates:
        print("No candidates in 'Applied' stage.")
        return {"processed": 0, "passed": 0, "failed": 0, "hard_filtered": 0}

    stats = {"processed": 0, "passed": 0, "failed": 0, "hard_filtered": 0}

    for page in candidates:
        page_id = page["id"]
        name = get_candidate_data(page)["full_name"]
        print(f"\nEvaluating: {name}")

        result = process_single_stage1(page_id, config)
        decision = result["decision"]

        if decision == "Hard Filter":
            stats["hard_filtered"] += 1
            print(f"  HARD FILTER -> Rejected: {result['reasoning']}")
        elif decision == "Pass":
            stats["passed"] += 1
            print(f"  PASS ({result['score']}/100) -> Stage 2")
        elif decision == "Fail":
            stats["failed"] += 1
            print(f"  FAIL ({result['score']}/100) -> Rejected")
        elif decision == "Already processed":
            print(f"  SKIPPED (already scored {result['score']})")
        else:
            print(f"  {decision}: {result['reasoning']}")

        stats["processed"] += 1
        time.sleep(1)

    print(f"\n--- Stage 1 Complete ---")
    print(f"Processed: {stats['processed']}, Hard Filtered: {stats['hard_filtered']}")
    print(f"Passed: {stats['passed']}, Failed: {stats['failed']}")
    return stats


# --- Stage 2: Systems competency (scored /20) ---

def run_stage2(config: dict) -> dict:
    """Evaluate Stage 2 systems competency submissions."""
    db_id = config["notion_database_id"]
    thresholds = config["thresholds"]
    stages = config["stages"]

    candidates = get_candidates(db_id, stage=stages["stage2_task"])
    if not candidates:
        print("No candidates in 'Stage 2 Task' stage.")
        return {"processed": 0, "passed": 0, "failed": 0}

    prompts = get_prompts(config)
    if len(prompts) < 4:
        print("ERROR: Stage 2 prompt not found (expected index 2-3).")
        return {"processed": 0, "passed": 0, "failed": 0}
    stage2_prompt = "\n".join(prompts[2:4])  # Two blocks: scoring + AI detection

    stats = {"processed": 0, "passed": 0, "failed": 0}

    for page in candidates:
        candidate = get_candidate_data(page)
        name = candidate["full_name"]
        print(f"\nEvaluating Stage 2: {name}")

        # Collect submissions per task — each scored independently
        notion_imgs = candidate.get("notion_screenshots", [])
        sheets_imgs = candidate.get("spreadsheet_screenshots", [])
        pres_imgs = candidate.get("presentation_screenshots", [])

        # Build labelled image list so AI knows which screenshots belong to which task
        all_images = []
        image_labels = []
        if notion_imgs:
            for img in notion_imgs:
                all_images.append(img)
                image_labels.append("TASK 1 - Notion Pipeline Screenshot")
        if sheets_imgs:
            for img in sheets_imgs:
                all_images.append(img)
                image_labels.append("TASK 2 - Spreadsheet Screenshot")
        if pres_imgs:
            for img in pres_imgs:
                all_images.append(img)
                image_labels.append("TASK 3 - Presentation Screenshot")

        # Collect text submission for Task 4 (AI email)
        prompts_text = candidate.get("ai_email_prompts", "")
        unedited_text = candidate.get("ai_email_unedited", "")
        edited_text = candidate.get("ai_email_edited", "")

        # Build submission context with clear labelling and missing flags
        text_parts = []
        text_parts.append("=== SUBMISSION STATUS ===")
        text_parts.append(f"Task 1 - Notion Pipeline: {'SUBMITTED ({} screenshot(s))'.format(len(notion_imgs)) if notion_imgs else 'NOT SUBMITTED — score 0 for this task'}")
        text_parts.append(f"Task 2 - Spreadsheet: {'SUBMITTED ({} screenshot(s))'.format(len(sheets_imgs)) if sheets_imgs else 'NOT SUBMITTED — score 0 for this task'}")
        text_parts.append(f"Task 3 - Presentation: {'SUBMITTED ({} screenshot(s))'.format(len(pres_imgs)) if pres_imgs else 'NOT SUBMITTED — score 0 for this task'}")
        text_parts.append(f"Task 4 - AI Email: {'SUBMITTED' if (prompts_text or unedited_text or edited_text) else 'NOT SUBMITTED — score 0 for this task'}")
        text_parts.append("")

        if image_labels:
            text_parts.append("=== IMAGE ORDER ===")
            for i, label in enumerate(image_labels, 1):
                text_parts.append(f"Image {i}: {label}")
            text_parts.append("")

        if prompts_text:
            text_parts.append(f"TASK 4 - AI Email Prompts:\n{prompts_text}")
        if unedited_text:
            text_parts.append(f"TASK 4 - AI Email (unedited AI output):\n{unedited_text}")
        if edited_text:
            text_parts.append(f"TASK 4 - AI Email (human-edited final version):\n{edited_text}")
        text_content = "\n\n".join(text_parts)

        # Fall back to single Stage 2 Submission field if no split fields
        fallback = candidate.get("stage2_submission", "")

        has_images = len(all_images) > 0
        has_text = bool(prompts_text or unedited_text or edited_text or fallback)

        if not has_images and not has_text:
            print(f"  SKIPPED — No Stage 2 submission found for {name}")
            continue

        # Use vision-capable evaluation if screenshots exist
        if has_images:
            print(f"    Sending {len(all_images)} images + text to Claude Vision...")
            print(f"    Tasks submitted: Notion={'Y' if notion_imgs else 'N'}, Sheets={'Y' if sheets_imgs else 'N'}, Pres={'Y' if pres_imgs else 'N'}, Email={'Y' if has_text else 'N'}")
            result = evaluate_with_images(
                prompt=stage2_prompt,
                image_urls=all_images,
                text_content=text_content or fallback,
            )
        else:
            # Text-only evaluation (no screenshots at all)
            submission = text_content or fallback
            result = evaluate_candidate(candidate, stage2_prompt, submission_text=submission)

        score = result.get("score", 0)
        decision = "Pass" if score >= thresholds["stage2_pass"] else "Fail"

        update_candidate(candidate["page_id"], {
            "Stage 2 Score": score,
            "AI Reasoning": result.get("reasoning", ""),
            "Strengths": result.get("strengths", []),
            "Weaknesses": result.get("weaknesses", []),
        })

        if decision == "Pass":
            advance_candidate(candidate["page_id"], stages["stage3_task"])
            set_email_action(candidate["page_id"], "Passed Stage 2")
            stats["passed"] += 1
            print(f"  PASS ({score}/20) -> Stage 3")
        else:
            reject_candidate(candidate["page_id"], result.get("reasoning", ""))
            set_email_action(candidate["page_id"], "Failed")
            stats["failed"] += 1
            print(f"  FAIL ({score}/20) -> Rejected")

        stats["processed"] += 1
        time.sleep(1)

    print(f"\n--- Stage 2 Complete ---")
    print(f"Processed: {stats['processed']}, Passed: {stats['passed']}, Failed: {stats['failed']}")
    return stats


# --- Stage 3: Executive sales simulation (scored /35) ---

def run_stage3(config: dict) -> dict:
    """Evaluate Stage 3 sales simulation submissions."""
    db_id = config["notion_database_id"]
    thresholds = config["thresholds"]
    stages = config["stages"]

    candidates = get_candidates(db_id, stage=stages["stage3_task"])
    if not candidates:
        print("No candidates in 'Stage 3 Task' stage.")
        return {"processed": 0, "passed": 0, "failed": 0}

    prompts = get_prompts(config)
    if len(prompts) < 6:
        print("ERROR: Stage 3 prompt not found (expected index 4-5).")
        return {"processed": 0, "passed": 0, "failed": 0}
    stage3_prompt = "\n".join(prompts[4:6])  # Two blocks: auto-fail checks + scoring

    stats = {"processed": 0, "passed": 0, "failed": 0}

    for page in candidates:
        candidate = get_candidate_data(page)
        name = candidate["full_name"]
        print(f"\nEvaluating Stage 3: {name}")

        submission = candidate.get("stage3_submission", "")
        if not submission:
            cv_urls = candidate.get("cv_upload") or []
            submission = fetch_cv_content(cv_urls[0]) if cv_urls else ""
        if not submission or submission.startswith("["):
            print(f"  SKIPPED — No Stage 3 submission found for {name}")
            continue

        result = evaluate_candidate(candidate, stage3_prompt, submission_text=submission)

        score = result.get("score", 0)
        decision = "Pass" if score >= thresholds["stage3_pass"] else "Fail"

        update_candidate(candidate["page_id"], {
            "Stage 3 Score": score,
            "AI Reasoning": result.get("reasoning", ""),
            "Strengths": result.get("strengths", []),
            "Weaknesses": result.get("weaknesses", []),
        })

        if decision == "Pass":
            advance_candidate(candidate["page_id"], stages["stage4_task"])
            set_email_action(candidate["page_id"], "Passed Stage 3")
            stats["passed"] += 1
            print(f"  PASS ({score}/35) -> Stage 4")
        else:
            reject_candidate(candidate["page_id"], result.get("reasoning", ""))
            set_email_action(candidate["page_id"], "Failed")
            stats["failed"] += 1
            print(f"  FAIL ({score}/35) -> Rejected")

        stats["processed"] += 1
        time.sleep(1)

    print(f"\n--- Stage 3 Complete ---")
    print(f"Processed: {stats['processed']}, Passed: {stats['passed']}, Failed: {stats['failed']}")
    return stats


# --- Stage 4: Technical understanding (scored /25) ---

def run_stage4(config: dict) -> dict:
    """Evaluate Stage 4 technical understanding submissions."""
    db_id = config["notion_database_id"]
    thresholds = config["thresholds"]
    stages = config["stages"]

    candidates = get_candidates(db_id, stage=stages["stage4_task"])
    if not candidates:
        print("No candidates in 'Stage 4 Task' stage.")
        return {"processed": 0, "passed": 0, "failed": 0}

    prompts = get_prompts(config)
    if len(prompts) < 8:
        print("ERROR: Stage 4 prompt not found (expected index 6-7).")
        return {"processed": 0, "passed": 0, "failed": 0}
    stage4_prompt = "\n".join(prompts[6:8])  # Two blocks: reference answers + scoring

    stats = {"processed": 0, "passed": 0, "failed": 0}

    for page in candidates:
        candidate = get_candidate_data(page)
        name = candidate["full_name"]
        print(f"\nEvaluating Stage 4: {name}")

        submission = candidate.get("stage4_submission", "")
        if not submission:
            cv_urls = candidate.get("cv_upload") or []
            submission = fetch_cv_content(cv_urls[0]) if cv_urls else ""
        if not submission or submission.startswith("["):
            print(f"  SKIPPED — No Stage 4 submission found for {name}")
            continue

        result = evaluate_candidate(candidate, stage4_prompt, submission_text=submission)

        score = result.get("score", 0)
        decision = "Pass" if score >= thresholds["stage4_pass"] else "Fail"

        update_candidate(candidate["page_id"], {
            "Stage 4 Score": score,
            "AI Reasoning": result.get("reasoning", ""),
            "Strengths": result.get("strengths", []),
            "Weaknesses": result.get("weaknesses", []),
        })

        if decision == "Pass":
            advance_candidate(candidate["page_id"], stages["stage5_task"])
            set_email_action(candidate["page_id"], "Passed Stage 4")
            stats["passed"] += 1
            print(f"  PASS ({score}/25) -> Stage 5")
        else:
            reject_candidate(candidate["page_id"], result.get("reasoning", ""))
            set_email_action(candidate["page_id"], "Failed")
            stats["failed"] += 1
            print(f"  FAIL ({score}/25) -> Rejected")

        stats["processed"] += 1
        time.sleep(1)

    print(f"\n--- Stage 4 Complete ---")
    print(f"Processed: {stats['processed']}, Passed: {stats['passed']}, Failed: {stats['failed']}")
    return stats


# --- Stage 5: Bitcoin execution (pass/fail) ---


def _build_blockchain_context(
    onchain_data: dict,
    lightning_data: dict,
    txid: str | None,
    payment_hash: str | None,
    onchain_amount_match: dict | None = None,
    lightning_amount_match: dict | None = None,
    claimed_onchain_sats: int | None = None,
    claimed_lightning_sats: int | None = None,
) -> str:
    """Format blockchain verification results as context for the AI prompt."""
    lines = ["=== BLOCKCHAIN VERIFICATION DATA ===", ""]

    # On-chain section
    lines.append("ON-CHAIN (mempool.space):")
    if onchain_data.get("error"):
        lines.append(f"  API Error: {onchain_data['error']}")
    else:
        tx_count = len(onchain_data.get("transactions", []))
        lines.append(f"  Transactions found to target address: {'Yes' if tx_count > 0 else 'No'}")
        lines.append(f"  Transaction count: {tx_count}")
        lines.append(f"  Total received: {onchain_data.get('total_received_sats', 0)} sats")
        if txid:
            match = onchain_data.get("txid_match")
            lines.append(f"  Candidate TXID ({txid[:16]}...): {'VERIFIED - Found on-chain' if match else 'NOT FOUND on-chain'}")
        if claimed_onchain_sats and onchain_amount_match:
            if onchain_amount_match["matched"]:
                best = onchain_amount_match.get("best_match", {})
                lines.append(f"  Candidate claimed {claimed_onchain_sats} sats on-chain: MATCHED (txid: {best.get('txid', 'N/A')[:16]}...)")
            else:
                lines.append(f"  Candidate claimed {claimed_onchain_sats} sats on-chain: NO MATCHING TRANSACTION FOUND")
        for tx in onchain_data.get("transactions", [])[:5]:
            status = "confirmed" if tx["confirmed"] else "unconfirmed"
            lines.append(f"  - txid: {tx['txid'][:16]}... | {tx['amount_sats']} sats | {status}")

    lines.append("")

    # Lightning section
    lines.append("LIGHTNING (Blink API):")
    if lightning_data.get("error"):
        lines.append(f"  API Error: {lightning_data['error']}")
    else:
        pay_count = len(lightning_data.get("recent_payments", []))
        lines.append(f"  Recent incoming payments found: {'Yes' if pay_count > 0 else 'No'}")
        lines.append(f"  Payment count: {pay_count}")
        if payment_hash:
            match = lightning_data.get("hash_match")
            lines.append(f"  Candidate payment hash ({payment_hash[:16]}...): {'VERIFIED - Hash matched' if match else 'NOT FOUND in records'}")
        if claimed_lightning_sats and lightning_amount_match:
            if lightning_amount_match["matched"]:
                best = lightning_amount_match.get("best_match", {})
                lines.append(f"  Candidate claimed {claimed_lightning_sats} sats via Lightning: MATCHED (hash: {(best.get('payment_hash') or 'N/A')[:16]}...)")
            else:
                lines.append(f"  Candidate claimed {claimed_lightning_sats} sats via Lightning: NO MATCHING PAYMENT FOUND")
        for pay in lightning_data.get("recent_payments", [])[:5]:
            lines.append(f"  - {pay['amount_sats']} sats | {pay['created_at']} | hash: {(pay.get('payment_hash') or 'N/A')[:16]}...")

    lines.append("")
    lines.append("VERIFICATION NOTES:")
    lines.append("- Cross-reference the screenshots with the blockchain data above")
    lines.append("- If the candidate provided a transaction ID / payment hash and it was verified, note this as extra credit")
    lines.append("- Amount matching is used to attribute specific transactions to this candidate")
    lines.append(f"- Target BTC address: {TARGET_BTC_ADDRESS}")
    lines.append("- Target Lightning address: useorange@blink.sv")

    return "\n".join(lines)


def _determine_stage5_result(ai_result: dict) -> tuple[str, int]:
    """Extract score and pass/fail from the AI's 6-point rubric evaluation.

    Returns (decision, score) tuple.
    """
    score = ai_result.get("score", 0)
    if not isinstance(score, int):
        try:
            score = int(score)
        except (ValueError, TypeError):
            score = 0

    # Fraud override
    if ai_result.get("fraud_flag"):
        return "Fail", 0

    decision = ai_result.get("decision", "Fail")
    if decision not in ("Pass", "Fail"):
        decision = "Pass" if score >= 4 else "Fail"

    # Enforce threshold regardless of AI decision
    if score < 4:
        decision = "Fail"

    return decision, score


def run_stage5(config: dict) -> dict:
    """Evaluate Stage 5 Bitcoin execution — on-chain + Lightning verification."""
    db_id = config["notion_database_id"]
    stages = config["stages"]

    candidates = get_candidates(db_id, stage=stages["stage5_task"])
    if not candidates:
        print("No candidates in 'Stage 5 Task' stage.")
        return {"processed": 0, "passed": 0, "failed": 0}

    prompts = get_prompts(config)
    if len(prompts) < 10:
        print("ERROR: Stage 5 prompt not found (expected indices 8-9).")
        return {"processed": 0, "passed": 0, "failed": 0}
    stage5_prompt = "\n".join(prompts[8:10])  # Two blocks: scoring rubric + output format

    blink_api_key = os.getenv("BLINK_API_KEY", "")

    stats = {"processed": 0, "passed": 0, "failed": 0}

    for page in candidates:
        candidate = get_candidate_data(page)
        name = candidate["full_name"]
        print(f"\nEvaluating Stage 5: {name}")

        # Gather submissions
        btc_screenshots = candidate.get("stage5_btc_screenshot") or []
        ln_screenshots = candidate.get("stage5_lightning_screenshot") or []
        txid = (candidate.get("stage5_btc_txid") or "").strip() or None
        payment_hash = (candidate.get("stage5_lightning_hash") or "").strip() or None

        # Self-reported amounts for transaction attribution
        # Parse robustly: strip commas, spaces, "sats" suffix, handle BTC notation
        def _parse_sats(raw: str) -> int | None:
            if not raw:
                return None
            # Remove common non-numeric text
            cleaned = raw.lower().replace(",", "").replace(" ", "")
            cleaned = cleaned.replace("sats", "").replace("sat", "").replace("satoshis", "").replace("satoshi", "")
            cleaned = cleaned.strip()
            if not cleaned:
                return None
            try:
                # Handle BTC notation (e.g. "0.00001000")
                val = float(cleaned)
                if val < 1:
                    # Likely BTC, convert to sats
                    return int(round(val * 100_000_000))
                return int(val)
            except ValueError:
                return None

        raw_onchain_sats = (candidate.get("stage5_onchain_sats") or "").strip()
        raw_lightning_sats = (candidate.get("stage5_lightning_sats") or "").strip()
        claimed_onchain_sats = _parse_sats(raw_onchain_sats)
        claimed_lightning_sats = _parse_sats(raw_lightning_sats)

        # Use Notion page last_edited_time as approximate submission timestamp
        submission_timestamp = None
        try:
            from datetime import datetime
            edited = page.get("last_edited_time", "")
            if edited:
                submission_timestamp = int(datetime.fromisoformat(edited.replace("Z", "+00:00")).timestamp())
        except Exception:
            pass

        if not btc_screenshots and not ln_screenshots and not txid and not payment_hash:
            print(f"  SKIPPED — No Stage 5 submissions found for {name}")
            continue

        # Blockchain verification
        print(f"  Verifying on-chain transaction...")
        onchain_data = verify_onchain_transaction(TARGET_BTC_ADDRESS, txid=txid)
        if onchain_data.get("verified"):
            print(f"  On-chain: {len(onchain_data['transactions'])} tx(s) found, {onchain_data['total_received_sats']} sats total")
            if txid and onchain_data.get("txid_match"):
                print(f"  TXID verified on-chain (extra credit)")
        elif onchain_data.get("error"):
            print(f"  On-chain API error: {onchain_data['error']}")
        else:
            print(f"  On-chain: No transactions found to target address")

        print(f"  Verifying Lightning payment...")
        if blink_api_key:
            lightning_data = verify_lightning_payment(blink_api_key, payment_hash=payment_hash)
            if lightning_data.get("verified"):
                print(f"  Lightning: {len(lightning_data['recent_payments'])} incoming payment(s) found")
                if payment_hash and lightning_data.get("hash_match"):
                    print(f"  Payment hash verified (extra credit)")
            elif lightning_data.get("error"):
                print(f"  Lightning API error: {lightning_data['error']}")
            else:
                print(f"  Lightning: No incoming payments found")
        else:
            lightning_data = {"verified": False, "recent_payments": [], "error": "No BLINK_API_KEY configured"}
            print(f"  Lightning: Skipped — no Blink API key")

        # Amount-based transaction attribution
        onchain_amount_match = None
        lightning_amount_match = None
        if claimed_onchain_sats:
            onchain_amount_match = match_onchain_by_amount(onchain_data, claimed_onchain_sats, submission_timestamp)
            if onchain_amount_match["matched"]:
                print(f"  On-chain amount match: {claimed_onchain_sats} sats found")
            else:
                print(f"  On-chain amount match: {claimed_onchain_sats} sats NOT found")
        if claimed_lightning_sats:
            lightning_amount_match = match_lightning_by_amount(lightning_data, claimed_lightning_sats, submission_timestamp)
            if lightning_amount_match["matched"]:
                print(f"  Lightning amount match: {claimed_lightning_sats} sats found")
            else:
                print(f"  Lightning amount match: {claimed_lightning_sats} sats NOT found")

        # Build context and evaluate with AI
        blockchain_context = _build_blockchain_context(
            onchain_data, lightning_data, txid, payment_hash,
            onchain_amount_match=onchain_amount_match,
            lightning_amount_match=lightning_amount_match,
            claimed_onchain_sats=claimed_onchain_sats,
            claimed_lightning_sats=claimed_lightning_sats,
        )
        all_images = (btc_screenshots or []) + (ln_screenshots or [])

        if all_images:
            result = evaluate_with_images(
                prompt=stage5_prompt,
                image_urls=all_images,
                text_content=blockchain_context,
            )
        else:
            # No screenshots — evaluate with blockchain data only
            result = evaluate_candidate(candidate, stage5_prompt, submission_text=blockchain_context)

        # Determine final result from 6-point rubric
        final_result, score = _determine_stage5_result(result)

        # Build detailed reasoning
        reasoning = result.get("reasoning", "")
        item_scores = result.get("item_scores", {})
        if item_scores:
            breakdown = ", ".join(f"{k}={v}" for k, v in item_scores.items())
            reasoning += f"\n[ITEM SCORES] {breakdown}"
        if result.get("fraud_flag"):
            reasoning += "\n[FRAUD FLAG] Screenshots appear fabricated — blockchain data shows no matching transactions."

        update_candidate(candidate["page_id"], {
            "Stage 5 Result": final_result,
            "Stage 5 Score": score,
            "AI Reasoning": reasoning,
        })

        if final_result == "Pass":
            advance_candidate(candidate["page_id"], stages["final_interview"])
            set_email_action(candidate["page_id"], "Passed Stage 5")
            # Only add to interview DB if not already there (duplicate-run protection)
            from notion_client import _request as _nr
            existing = _nr("POST", f"/databases/{INTERVIEW_DB_ID}/query", {
                "filter": {"property": "Source Page", "url": {"equals": f"https://www.notion.so/{candidate['page_id'].replace('-', '')}"}},
                "page_size": 1,
            })
            if not existing.get("results"):
                add_to_interview_database(candidate, page, stage5_score=score)
            else:
                print(f"  Already in Interview DB — skipping duplicate")
            stats["passed"] += 1
            print(f"  PASS ({score}/6) -> Final Interview")
        else:
            reject_candidate(candidate["page_id"], reasoning or "Failed Bitcoin execution test")
            set_email_action(candidate["page_id"], "Failed")
            stats["failed"] += 1
            print(f"  FAIL ({score}/6) -> Rejected")

        stats["processed"] += 1
        time.sleep(1)

    print(f"\n--- Stage 5 Complete ---")
    print(f"Processed: {stats['processed']}, Passed: {stats['passed']}, Failed: {stats['failed']}")
    return stats


# --- Timeout Check ---

def run_timeout_check(config: dict) -> dict:
    """Check active candidates for 7-day timeout since application creation.

    - At 5 days: send warning email if not already warned.
    - At 7 days: reject candidate and send expiry email.
    Only applies to candidates in stages 2-5.
    """
    from datetime import datetime, timezone

    db_id = config["notion_database_id"]
    timeout_cfg = config.get("timeout", {})
    warning_days = timeout_cfg.get("warning_days", 5)
    expiry_days = timeout_cfg.get("expiry_days", 7)
    stages = config["stages"]

    active_stages = [
        stages["stage2_task"],
        stages["stage3_task"],
        stages["stage4_task"],
        stages["stage5_task"],
    ]

    now = datetime.now(timezone.utc)
    stats = {"checked": 0, "warned": 0, "expired": 0}

    for stage_name in active_stages:
        candidates = get_candidates(db_id, stage=stage_name)
        for page in candidates:
            stats["checked"] += 1
            candidate = get_candidate_data(page)
            name = candidate["full_name"]

            # Use page created_time as application start timestamp
            created_str = page.get("created_time", "")
            if not created_str:
                print(f"  SKIP {name}: no created_time")
                continue

            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            elapsed_days = (now - created).total_seconds() / 86400

            # Read current Email Action to avoid duplicate warnings
            props = page.get("properties", {})
            ea_prop = props.get("Email Action", {}).get("select")
            current_action = ea_prop.get("name") if ea_prop else None

            if elapsed_days >= expiry_days:
                print(f"  TIMEOUT EXPIRED: {name} ({elapsed_days:.1f} days, in {stage_name})")
                reject_candidate(candidate["page_id"], "Application timed out: 7-day deadline exceeded")
                set_email_action(candidate["page_id"], "Timeout Expired")
                stats["expired"] += 1

            elif elapsed_days >= warning_days and current_action != "Timeout Warning":
                print(f"  TIMEOUT WARNING: {name} ({elapsed_days:.1f} days, in {stage_name})")
                set_email_action(candidate["page_id"], "Timeout Warning")
                stats["warned"] += 1

            else:
                print(f"  OK: {name} ({elapsed_days:.1f} days, in {stage_name})")

        time.sleep(0.35)

    print(f"\n--- Timeout Check Complete ---")
    print(f"Checked: {stats['checked']}, Warned: {stats['warned']}, Expired: {stats['expired']}")
    return stats


# --- Final Ranking ---

def run_ranking(config: dict) -> dict:
    """Generate final ranked shortlist with normalized composite scores."""
    db_id = config["notion_database_id"]
    weights = config["composite_weights"]
    max_scores = config.get("max_scores", {"stage1": 100, "stage2": 20, "stage3": 35, "stage4": 25})
    min_finalists = config["min_finalists"]
    stages = config["stages"]

    finalists = get_candidates(db_id, stage=stages["final_interview"])
    if not finalists:
        print("No candidates in 'Final Interview' stage.")
        return {"ranked": 0}

    prompts = get_prompts(config)
    ranking_prompt = prompts[10] if len(prompts) >= 11 else None

    candidates_for_ranking = []
    for page in finalists:
        props = page["properties"]
        candidate = get_candidate_data(page)

        s1 = props.get("AI Score Stage 1", {}).get("number") or 0
        s2 = props.get("Stage 2 Score", {}).get("number") or 0
        s3 = props.get("Stage 3 Score", {}).get("number") or 0
        s4 = props.get("Stage 4 Score", {}).get("number") or 0

        # Normalize to 0-100
        s1_norm = (s1 / max_scores["stage1"]) * 100
        s2_norm = (s2 / max_scores["stage2"]) * 100
        s3_norm = (s3 / max_scores["stage3"]) * 100
        s4_norm = (s4 / max_scores["stage4"]) * 100

        # Composite (interview defaults to 0 until entered)
        composite = (
            s1_norm * weights.get("stage1", 0)
            + s2_norm * weights.get("stage2", 0)
            + s3_norm * weights.get("stage3", 0)
            + s4_norm * weights.get("stage4", 0)
        )

        candidates_for_ranking.append({
            "page_id": candidate["page_id"],
            "name": candidate["full_name"],
            "stage1_score": s1,
            "stage2_score": s2,
            "stage3_score": s3,
            "stage4_score": s4,
            "composite_score": round(composite, 1),
        })

        update_candidate(candidate["page_id"], {"Final Score": round(composite, 1)})
        time.sleep(0.35)

    candidates_for_ranking.sort(key=lambda c: c["composite_score"], reverse=True)

    if ranking_prompt:
        ai_ranking = evaluate_ranking(candidates_for_ranking, ranking_prompt)
        print("\n--- AI Ranking Result ---")
        shortlist = ai_ranking.get("shortlist", [])
        for entry in shortlist:
            print(f"  #{entry.get('rank', '?')}: {entry.get('name', '?')} — Score: {entry.get('final_score', '?')}")
            print(f"    {entry.get('summary', '')}")
        if ai_ranking.get("notes"):
            print(f"\nNotes: {ai_ranking['notes']}")
    else:
        print("\n--- Composite Score Ranking ---")
        for i, c in enumerate(candidates_for_ranking, 1):
            print(f"  #{i}: {c['name']} — Composite: {c['composite_score']}")
            print(f"    S1:{c['stage1_score']}/100  S2:{c['stage2_score']}/20  S3:{c['stage3_score']}/35  S4:{c['stage4_score']}/25")

    if len(candidates_for_ranking) < min_finalists:
        print(f"\nWARNING: Only {len(candidates_for_ranking)} finalists. Minimum is {min_finalists}.")
        print("Consider reviewing candidates in 'Manual Review' stage.")

    return {"ranked": len(candidates_for_ranking)}
