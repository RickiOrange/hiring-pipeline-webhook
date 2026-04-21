"""Local synthetic tests for the stage-submission auto-merge logic.

Runs without touching Notion: constructs fake candidate/page dicts that
mimic what get_candidate_data / get_page would return, then exercises the
detection and matching code paths.

Usage:
    python test_stage_submission_merge.py
"""

import sys
from pipeline import (
    STAGE_SUBMISSION_SPECS,
    _detect_stage_submission,
    _normalize_name,
    _find_original_candidate,
    _is_past_stage,
    _stage_order_index,
)


# --- Helpers to build fake Notion pages ---

def _text_prop(value: str):
    return {"rich_text": [{"plain_text": value}]} if value else {"rich_text": []}

def _file_prop(names: list[str]):
    return {"files": [{"name": n, "type": "file", "file": {"url": f"https://example.com/{n}"}}
                      for n in names]}

def _title_prop(value: str):
    return {"title": [{"plain_text": value}]}

def _email_prop(value):
    return {"email": value}

def _select_prop(value):
    return {"select": {"name": value}} if value else {"select": None}

def _number_prop(value):
    return {"number": value}


def fake_stage1_application(name="Jane Doe", email="jane@example.com"):
    """Looks like a fresh Stage 1 application — has CV, writing, demographics."""
    return {
        "id": "page-s1-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Upload your CV": _file_prop(["cv.pdf"]),
            'Write two paragraphs about your favourite book and why you enjoyed it"':
                _text_prop("I enjoyed reading..."),
            "How old are you?": _select_prop("31-35"),
            "How many years of sales experience do you have?": _select_prop("8-10"),
        },
    }


def fake_stage2_orphan(name="Jane Doe", email="jane@example.com"):
    """Looks like a Stage 2 form submission — no Stage 1 data, has screenshots."""
    return {
        "id": "page-s2-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Notion task screenshots": _file_prop(["screen1.png"]),
            "Spreadsheet task screenshots": _file_prop(["sheet1.png"]),
            "A.I. email draft (1/3) - prompts": _text_prop("prompt text"),
            # No CV, no writing, no age, no experience
        },
    }


def fake_stage3_orphan(name="Jane Doe", email="jane@example.com"):
    """Legacy text-only Stage 3 orphan (covers the path where the form text
    field is filled directly rather than via file upload)."""
    return {
        "id": "page-s3-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Stage 3 Submission": _text_prop("my sales process is..."),
        },
    }


def fake_stage3_file_orphan(name="Jane Doe", email="jane@example.com",
                            filename="response.docx"):
    """Stage 3 orphan with the actual form payload: a single uploaded document
    in 'Upload your task' (which is also a Stage 2 field — disambiguation
    relies on the candidate's current Stage)."""
    return {
        "id": "page-s3-file-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Upload your task": _file_prop([filename]),
        },
    }


def fake_stage4_orphan(name="Jane Doe", email="jane@example.com"):
    """Stage 4 orphan — 9 separate concept text fields populated, no Stage 1 data."""
    return {
        "id": "page-s4-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Bitcoin — What is it and how does it work at a high level?":
                _text_prop("Bitcoin is a decentralized digital currency..."),
            "Lightning Network — What is it and what problem does it solve?":
                _text_prop("Lightning enables fast, cheap Bitcoin payments..."),
            "API — What is an API and how is it used in software?":
                _text_prop("An API is a contract between two services..."),
        },
    }


def fake_stage5_orphan(name="Jane Doe", email="jane@example.com"):
    return {
        "id": "page-s5-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "On-chain transaction ID": _text_prop("abc123"),
            "How many Satoshis did you send on-chain?": _text_prop("10000"),
        },
    }


def fake_original_with_score(name, email, score=75, s1_score=True):
    p = fake_stage1_application(name=name, email=email)
    if s1_score:
        p["properties"]["AI Score Stage 1"] = _number_prop(score)
    p["id"] = f"page-orig-{email}"
    return p


# --- get_candidate_data equivalent (mirrors the notion_client helper) ---

def _fake_get_candidate_data(page):
    """Minimal stand-in for notion_client.get_candidate_data.

    Only pulls the fields the matcher and detector actually read, to keep the
    test self-contained.
    """
    props = page["properties"]
    def _title(p): return "".join(t.get("plain_text", "") for t in p.get("title", []))
    def _rt(p): return "".join(t.get("plain_text", "") for t in p.get("rich_text", []))
    def _email(p): return p.get("email")
    def _sel(p):
        s = p.get("select")
        return s.get("name") if s else None
    def _files(p): return [f.get("name") for f in p.get("files", [])]

    return {
        "page_id": page["id"],
        "full_name": _title(props.get("Full Name", {})),
        "email": _email(props.get("What is your Email?", {})),
        "cv_upload": _files(props.get("Upload your CV", {})),
        "writing_test": _rt(props.get('Write two paragraphs about your favourite book and why you enjoyed it"', {})),
        "age_range": _sel(props.get("How old are you?", {})),
        "years_sales_range": _sel(props.get("How many years of sales experience do you have?", {})),
        "notion_screenshots": _files(props.get("Notion task screenshots", {})),
        "spreadsheet_screenshots": _files(props.get("Spreadsheet task screenshots", {})),
        "presentation_screenshots": _files(props.get("Presentation task screenshots (each slide)", {})),
        "ai_email_prompts": _rt(props.get("A.I. email draft (1/3) - prompts", {})),
        "ai_email_unedited": _rt(props.get("A.I. email draft (2/3) - non edited version", {})),
        "ai_email_edited": _rt(props.get("A.I. email draft (3/3) - edited version", {})),
        "stage2_submission": _rt(props.get("Stage 2 Submission", {})),
        "stage3_submission": _rt(props.get("Stage 3 Submission", {})),
        "stage4_submission": _rt(props.get("Stage 4 Submission", {})),
    }


# --- Tests ---

FAILURES = []


def check(name, cond, detail=""):
    status = "OK" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def test_normalize_name():
    print("\n== _normalize_name ==")
    check("strips trailing whitespace",
          _normalize_name("Benzile Makhanya ") == "benzile makhanya")
    check("strips leading whitespace",
          _normalize_name("  Patrick") == "patrick")
    check("collapses internal double-spaces",
          _normalize_name("Jane  Doe") == "jane doe")
    check("lowercases",
          _normalize_name("JANE DOE") == "jane doe")
    check("handles None",
          _normalize_name(None) == "")
    check("handles empty string",
          _normalize_name("") == "")


def test_detect_stage_submission():
    print("\n== _detect_stage_submission ==")

    # Stage 1 application → None
    p = fake_stage1_application()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Stage 1 application → None", spec is None,
          f"got {spec['label'] if spec else None}")

    # Stage 2 orphan → Stage 2
    p = fake_stage2_orphan()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Stage 2 orphan → Stage 2",
          spec is not None and spec["label"] == "Stage 2",
          f"got {spec['label'] if spec else None}")

    # Stage 3 orphan → Stage 3
    p = fake_stage3_orphan()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Stage 3 orphan → Stage 3",
          spec is not None and spec["label"] == "Stage 3",
          f"got {spec['label'] if spec else None}")

    # Stage 5 orphan → Stage 5
    p = fake_stage5_orphan()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Stage 5 orphan → Stage 5",
          spec is not None and spec["label"] == "Stage 5",
          f"got {spec['label'] if spec else None}")

    # Fully populated original at Stage 2 Task → None (has Stage 1 data)
    p = fake_stage1_application()
    p["properties"]["Notion task screenshots"] = _file_prop(["s1.png"])
    p["properties"]["Stage 2 Submission"] = _text_prop("sub")
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Populated original with stage 2 fields → None (Stage 1 data wins)",
          spec is None,
          f"got {spec['label'] if spec else None}")

    # Stage 4 orphan → Stage 4
    p = fake_stage4_orphan()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("Stage 4 orphan (concept fields) → Stage 4",
          spec is not None and spec["label"] == "Stage 4",
          f"got {spec['label'] if spec else None}")

    # File-only Stage 3 orphan ("Upload your task") with NO current_stage hint:
    # Stage 2 file_props also include "Upload your task", so both Stage 2 and
    # Stage 3 specs match. Without disambiguation we fall back to the latest
    # spec in the list (Stage 3) — acceptable but not authoritative.
    p = fake_stage3_file_orphan()
    c = _fake_get_candidate_data(p)
    spec = _detect_stage_submission(c, p["properties"])
    check("File-only orphan, no hint → Stage 3 (latest match wins)",
          spec is not None and spec["label"] == "Stage 3",
          f"got {spec['label'] if spec else None}")

    # File-only orphan with current_stage='Stage 2 Task' → Stage 2 (correctly disambiguated)
    spec = _detect_stage_submission(c, p["properties"], current_stage="Stage 2 Task")
    check("File-only orphan, hint=Stage 2 Task → Stage 2",
          spec is not None and spec["label"] == "Stage 2",
          f"got {spec['label'] if spec else None}")

    # File-only orphan with current_stage='Stage 3 Task' → Stage 3
    spec = _detect_stage_submission(c, p["properties"], current_stage="Stage 3 Task")
    check("File-only orphan, hint=Stage 3 Task → Stage 3",
          spec is not None and spec["label"] == "Stage 3",
          f"got {spec['label'] if spec else None}")

    # current_stage hint that matches no populated spec → falls back to last match
    spec = _detect_stage_submission(c, p["properties"], current_stage="Stage 5 Task")
    check("File-only orphan, hint=Stage 5 Task (no payload for it) → falls back to Stage 3",
          spec is not None and spec["label"] == "Stage 3",
          f"got {spec['label'] if spec else None}")


def test_find_original_candidate():
    print("\n== _find_original_candidate (via monkey-patched get_candidates) ==")

    # Monkey-patch get_candidates in pipeline to return a controlled fixture
    import pipeline
    original_get_candidates = pipeline.get_candidates
    original_get_candidate_data = pipeline.get_candidate_data
    try:
        pipeline.get_candidate_data = _fake_get_candidate_data

        # --- Case 1: Exact match with trailing-space discrepancy
        db = [fake_original_with_score("Benzile Makhanya ", "benzile@live.com")]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Benzile Makhanya", "benzile@live.com", "DB", exclude_page_id="other")
        check("Trailing space on original, clean name on orphan → matches",
              match is not None and match["id"] == db[0]["id"],
              f"got {match['id'] if match else None}")

        # --- Case 2: Multiple same-name rows, email disambiguates
        db = [
            fake_original_with_score("Jane Doe", "jane1@x.com"),
            fake_original_with_score("Jane Doe", "jane2@x.com"),
        ]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Jane Doe", "jane2@x.com", "DB", exclude_page_id="other")
        check("Two Jane Does, email picks the right one",
              match is not None and match["id"] == db[1]["id"],
              f"got {match['id'] if match else None}")

        # --- Case 3: First-name-only, email resolves
        db = [fake_original_with_score("Patrick Cisuaka Beya", "patbeya@gmail.com")]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Patrick", "patbeya@gmail.com", "DB", exclude_page_id="other")
        check("Only first name typed, email finds the match",
              match is not None and match["id"] == db[0]["id"],
              f"got {match['id'] if match else None}")

        # --- Case 4: First-name-only, wrong email → None
        db = [fake_original_with_score("Patrick Cisuaka Beya", "patbeya@gmail.com")]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Patrick", "someoneelse@x.com", "DB", exclude_page_id="other")
        check("First name only, email mismatch → None",
              match is None,
              f"got {match['id'] if match else None}")

        # --- Case 5: Orphan excluded from candidate pool
        orig = fake_original_with_score("Jane Doe", "jane@x.com")
        orphan = fake_stage2_orphan("Jane Doe", "jane@x.com")
        db = [orig, orphan]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Jane Doe", "jane@x.com", "DB", exclude_page_id=orphan["id"])
        check("Orphan page is excluded from candidate pool",
              match is not None and match["id"] == orig["id"],
              f"got {match['id'] if match else None}")

        # --- Case 6: Case insensitivity
        db = [fake_original_with_score("Jane Doe", "JANE@X.com")]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "jane doe", "jane@x.com", "DB", exclude_page_id="other")
        check("Case-insensitive name + email match",
              match is not None and match["id"] == db[0]["id"],
              f"got {match['id'] if match else None}")

        # --- Case 7: No candidate → None
        db = []
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Ghost Person", "ghost@x.com", "DB", exclude_page_id="other")
        check("Empty DB → None",
              match is None,
              f"got {match['id'] if match else None}")

        # --- Case 8: Archived candidate ignored
        arch = fake_original_with_score("Jane Doe", "jane@x.com")
        arch["archived"] = True
        db = [arch]
        pipeline.get_candidates = lambda _: db
        match = pipeline._find_original_candidate(
            "Jane Doe", "jane@x.com", "DB", exclude_page_id="other")
        check("Archived candidate not matched",
              match is None,
              f"got {match['id'] if match else None}")

    finally:
        pipeline.get_candidates = original_get_candidates
        pipeline.get_candidate_data = original_get_candidate_data


def test_is_past_stage():
    print("\n== _is_past_stage (game-prevention) ==")
    spec_s2 = next(s for s in STAGE_SUBMISSION_SPECS if s["label"] == "Stage 2")
    spec_s3 = next(s for s in STAGE_SUBMISSION_SPECS if s["label"] == "Stage 3")

    def _props_with_stage(name):
        return {"Stage": {"select": {"name": name}}} if name else {"Stage": {"select": None}}

    # Candidate at Stage 2 Task, re-submits Stage 2 → NOT past
    is_past, cur = _is_past_stage(_props_with_stage("Stage 2 Task"), spec_s2)
    check("At Stage 2 Task, re-submits Stage 2 → not past", not is_past, f"cur={cur}")

    # Candidate at Stage 3 Task, re-submits Stage 2 → past (BLOCK)
    is_past, cur = _is_past_stage(_props_with_stage("Stage 3 Task"), spec_s2)
    check("At Stage 3 Task, re-submits Stage 2 → past (block)", is_past, f"cur={cur}")

    # Candidate at Stage 4 Task, re-submits Stage 2 → past (BLOCK)
    is_past, cur = _is_past_stage(_props_with_stage("Stage 4 Task"), spec_s2)
    check("At Stage 4 Task, re-submits Stage 2 → past (block)", is_past, f"cur={cur}")

    # Candidate at Hired, re-submits Stage 3 → past (BLOCK)
    is_past, cur = _is_past_stage(_props_with_stage("Hired"), spec_s3)
    check("At Hired, re-submits Stage 3 → past (block)", is_past, f"cur={cur}")

    # Candidate at Stage 3 Task, re-submits Stage 3 → NOT past (legit mid-stage resub)
    is_past, cur = _is_past_stage(_props_with_stage("Stage 3 Task"), spec_s3)
    check("At Stage 3 Task, re-submits Stage 3 → not past", not is_past, f"cur={cur}")

    # Candidate at Applied, re-submits Stage 2 → NOT past (earlier than target)
    is_past, cur = _is_past_stage(_props_with_stage("Applied"), spec_s2)
    check("At Applied, re-submits Stage 2 → not past", not is_past, f"cur={cur}")

    # Unknown stage (e.g. Rejected) → treated as not past
    is_past, cur = _is_past_stage(_props_with_stage("Rejected"), spec_s2)
    check("At Rejected, re-submits Stage 2 → not blocked (unknown order)", not is_past, f"cur={cur}")

    # No Stage set at all → not past
    is_past, cur = _is_past_stage(_props_with_stage(None), spec_s2)
    check("No Stage set, re-submits Stage 2 → not past", not is_past, f"cur={cur}")

    # Stage ordering sanity
    check("STAGE_ORDER: Stage 3 Task > Stage 2 Task",
          _stage_order_index("Stage 3 Task") > _stage_order_index("Stage 2 Task"))
    check("STAGE_ORDER: Hired > Stage 5 Task",
          _stage_order_index("Hired") > _stage_order_index("Stage 5 Task"))
    check("STAGE_ORDER: unknown value → -1",
          _stage_order_index("Bogus") == -1)


def test_extended_deadline_logic():
    """Test the Extended Deadline branch in run_timeout_check — specifically
    that the (deadline + 24h) buffer is applied and warning fires 2 days before."""
    from datetime import datetime, timezone, timedelta
    print("\n== Extended Deadline logic ==")

    def _evaluate(deadline_str: str, now: datetime) -> str:
        """Replicate the branch logic from pipeline.run_timeout_check."""
        try:
            deadline_at = datetime.fromisoformat(deadline_str.replace("Z", "+00:00"))
        except ValueError:
            return "SKIP"
        if deadline_at.tzinfo is None:
            deadline_at = deadline_at.replace(tzinfo=timezone.utc)
        deadline_at = deadline_at + timedelta(days=1)  # end-of-day buffer
        warning_at = deadline_at - timedelta(days=2)
        if now >= deadline_at:
            return "EXPIRED"
        if now >= warning_at:
            return "WARNING"
        return "OK"

    # Reference "now" for deterministic tests: 2026-04-21 14:00 UTC
    now = datetime(2026, 4, 21, 14, 0, tzinfo=timezone.utc)

    # Benzile-style extension: deadline 2026-04-24 (Friday), now is Tuesday
    check("Deadline 3 days away (Benzile case) → OK",
          _evaluate("2026-04-24", now) == "OK",
          f"got {_evaluate('2026-04-24', now)}")

    # Deadline is today — still within the +24h buffer (not expired), but inside
    # the 2-day warning window → WARNING
    check("Deadline today (buffered, but <2d to expiry) → WARNING",
          _evaluate("2026-04-21", now) == "WARNING",
          f"got {_evaluate('2026-04-21', now)}")

    # Deadline yesterday — buffer ended at midnight today; now is 14:00 past that → EXPIRED
    check("Deadline yesterday (buffer expired) → EXPIRED",
          _evaluate("2026-04-20", now) == "EXPIRED",
          f"got {_evaluate('2026-04-20', now)}")

    # Deadline tomorrow — warning_at is yesterday 00:00, now is past it → WARNING
    check("Deadline tomorrow (within 2-day warning window) → WARNING",
          _evaluate("2026-04-22", now) == "WARNING",
          f"got {_evaluate('2026-04-22', now)}")

    # Deadline 5 days away — OK
    check("Deadline 5 days away → OK",
          _evaluate("2026-04-26", now) == "OK",
          f"got {_evaluate('2026-04-26', now)}")

    # Unparseable string → SKIP (no exception)
    check("Invalid deadline string → SKIP, no crash",
          _evaluate("not-a-date", now) == "SKIP",
          f"got {_evaluate('not-a-date', now)}")

    # ISO timestamp with time component — behaves the same (start-of-day + 24h)
    check("Deadline with time component → OK when 3 days away",
          _evaluate("2026-04-24T00:00:00Z", now) == "OK",
          f"got {_evaluate('2026-04-24T00:00:00Z', now)}")


def test_get_date_helper():
    """The _get_date helper in notion_client should return None for empty
    date properties and the start string for populated ones."""
    print("\n== _get_date helper ==")
    import notion_client as nc

    check("Empty date prop → None",
          nc._get_date({}) is None)
    check("Date prop with date=None → None",
          nc._get_date({"date": None}) is None)
    check("Populated date prop → start string",
          nc._get_date({"date": {"start": "2026-04-24", "end": None}}) == "2026-04-24")
    check("get_candidate_data returns extended_deadline key",
          "extended_deadline" in _fake_get_candidate_data_full())


def _fake_get_candidate_data_full() -> dict:
    """Build a minimal Notion page with Extended Deadline set and confirm
    the real get_candidate_data extracts it."""
    import notion_client as nc
    page = {
        "id": "test-page-id",
        "properties": {
            "Full Name": _title_prop("Test Person"),
            "Extended Deadline": {"date": {"start": "2026-04-24", "end": None}},
        },
    }
    return nc.get_candidate_data(page)


def test_specs_have_submitted_at():
    print("\n== STAGE_SUBMISSION_SPECS shape ==")
    required_keys = ("label", "score_prop", "stage_task", "submitted_at_prop",
                     "file_props", "text_props")
    for spec in STAGE_SUBMISSION_SPECS:
        for key in required_keys:
            check(f"{spec.get('label', '?')}: has '{key}'",
                  key in spec,
                  f"keys={list(spec.keys())}")
        # submitted_at_prop should follow the 'Stage N Submitted At' convention
        label = spec.get("label", "")
        expected = f"{label} Submitted At"
        check(f"{label}: submitted_at_prop = {expected!r}",
              spec.get("submitted_at_prop") == expected,
              f"got {spec.get('submitted_at_prop')!r}")


def main():
    test_normalize_name()
    test_detect_stage_submission()
    test_find_original_candidate()
    test_is_past_stage()
    test_extended_deadline_logic()
    test_get_date_helper()
    test_specs_have_submitted_at()

    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} test(s): {FAILURES}")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
