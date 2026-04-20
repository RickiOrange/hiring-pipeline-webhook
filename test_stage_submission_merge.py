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
    return {
        "id": "page-s3-orphan-001",
        "properties": {
            "Full Name": _title_prop(name),
            "What is your Email?": _email_prop(email),
            "Stage 3 Submission": _text_prop("my sales process is..."),
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


def main():
    test_normalize_name()
    test_detect_stage_submission()
    test_find_original_candidate()

    print()
    if FAILURES:
        print(f"FAILED: {len(FAILURES)} test(s): {FAILURES}")
        sys.exit(1)
    else:
        print("All tests passed.")


if __name__ == "__main__":
    main()
