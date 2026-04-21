"""Generic Notion API client for the AI hiring system."""

import httpx
import os
import time
from dotenv import load_dotenv

load_dotenv(override=True)

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_VERSION = "2022-06-28"
BASE_URL = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": NOTION_VERSION,
}


def _request(method: str, path: str, json_body: dict | None = None) -> dict:
    """Make an authenticated request to the Notion API."""
    url = f"{BASE_URL}{path}"
    with httpx.Client(timeout=30) as client:
        resp = client.request(method, url, headers=HEADERS, json=json_body)
        resp.raise_for_status()
        return resp.json()


def get_page(page_id: str) -> dict:
    """Fetch a single Notion page by ID."""
    return _request("GET", f"/pages/{page_id}")


def get_candidates(database_id: str, stage: str | None = None) -> list[dict]:
    """Query candidates from a database, optionally filtered by stage."""
    body: dict = {"page_size": 100}
    if stage:
        body["filter"] = {
            "property": "Stage",
            "select": {"equals": stage},
        }
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor:
            body["start_cursor"] = start_cursor
        data = _request("POST", f"/databases/{database_id}/query", body)
        results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        if has_more:
            time.sleep(0.35)

    return results


def get_candidate_data(page: dict) -> dict:
    """Extract candidate fields from a Notion page into a flat dict."""
    props = page["properties"]
    return {
        "page_id": page["id"],
        # Basic info
        "full_name": _get_title(props.get("Full Name", {})),
        "email": _get_email(props.get("What is your Email?", {})),
        "linkedin": _get_url(props.get("LinkedIn profile", {})),
        "cv_upload": _get_files(props.get("Upload your CV", {})),
        # Demographics
        "gender": _get_select(props.get("Gender", {})),
        "age_range": _get_select(props.get("How old are you?", {})),
        "based_in": _get_select(props.get("Where are you currently based?", {})),
        "willing_to_travel": _get_select(props.get("Are you willing to travel Africa for work?", {})),
        # Core qualifications
        "university_degree": _get_select(props.get("Do you have a University Degree?", {})),
        "years_sales_range": _get_select(props.get("How many years of sales experience do you have?", {})),
        "executive_sales": _get_select(props.get("Do you have executive sales experience", {})),
        "closed_deals_over_r1m": _get_select(props.get("Have you ever closed a deals valued at over R1M?", {})),
        # CRM / Pipeline
        "built_pipeline": _get_select(props.get("Have you ever built a sales pipeline from scratch yourself?", {})),
        "tracked_leads": _get_select(props.get("Have you ever tracked leads End-to-End in a CRM before?", {})),
        "created_deal_stages": _get_select(props.get("Have you ever created deal stages in a CRM", {})),
        "managed_followups": _get_select(props.get("Have you ever Managed Follow-Ups in CRM", {})),
        "maintained_crm_notes": _get_select(props.get("Have you ever maintained CRM notes on clients before?", {})),
        "crm_tools": _get_multi_select(props.get("What CRM Tools have you Used", {})),
        # Leadership
        "managed_sales_team": _get_select(props.get("Have you ever managed a Sales Team before?", {})),
        "hired_sales_staff": _get_select(props.get("Have you ever been in charge of Hiring a sales team?", {})),
        "run_weekly_meetings": _get_select(props.get("Have you ever run weekly sales meetings with a sales team?", {})),
        # Startup
        "worked_at_startup": _get_select(props.get("Have you ever worked at a Startup?", {})),
        "worked_under_50": _get_select(props.get("Have you ever worked at a company that employed less than 10 people before?", {})),
        "comfortable_no_playbook": _get_select(props.get("Are you comfortable working without a structured playbook?", {})),
        # Domain
        "fintech_experience": _get_select(props.get("Do you have Fintech Industry Experience?", {})),
        "payments_experience": _get_select(props.get("Do you have payments industry experience?", {})),
        "cross_border_experience": _get_select(props.get("Do you have cross-border payments experience?", {})),
        "african_market_experience": _get_select(props.get("Do you have African Market Experience", {})),
        # Excel
        "excel_sumif": _get_select(props.get("What does the SUMIF function in Excel/Sheets do?", {})),
        "excel_count": _get_select(props.get("What does a Excel COUNT function do?", {})),
        "excel_max": _get_select(props.get("What does the Excel MAX function do?", {})),
        "built_sales_spreadsheets": _get_select(props.get("Have you ever built sales reports spreadsheets?", {})),
        # Writing test
        "writing_test": _get_rich_text(props.get('Write two paragraphs about your favourite book and why you enjoyed it"', {})),
        # Stage 2 submissions (split across multiple fields)
        "stage2_submission": _get_rich_text(props.get("Stage 2 Submission", {})),
        "notion_screenshots": _get_files(props.get("Notion task screenshots", {})),
        "spreadsheet_screenshots": _get_files(props.get("Spreadsheet task screenshots", {})),
        "presentation_screenshots": _get_files(props.get("Presentation task screenshots (each slide)", {})),
        "ai_email_prompts": _get_rich_text(props.get("A.I. email draft (1/3) - prompts", {})),
        "ai_email_unedited": _get_rich_text(props.get("A.I. email draft (2/3) - non edited version", {})),
        "ai_email_edited": _get_rich_text(props.get("A.I. email draft (3/3) - edited version", {})),
        # Stage 3 & 4 submissions
        "stage3_submission": _get_rich_text(props.get("Stage 3 Submission", {})),
        "stage4_submission": _get_rich_text(props.get("Stage 4 Submission", {})),
        # Stage 5 submissions (check both naming conventions)
        "stage5_btc_screenshot": (
            _get_files(props.get("Stage 5 BTC Screenshot", {}))
            or _get_files(props.get("On-chain transaction screenshot", {}))
        ),
        "stage5_btc_txid": (
            _get_rich_text(props.get("Stage 5 BTC Transaction ID", {}))
            or _get_rich_text(props.get("On-chain transaction ID", {}))
        ),
        "stage5_lightning_screenshot": (
            _get_files(props.get("Stage 5 Lightning Screenshot", {}))
            or _get_files(props.get("Lightning payment screenshot", {}))
        ),
        "stage5_lightning_hash": (
            _get_rich_text(props.get("Stage 5 Lightning Payment Hash", {}))
            or _get_rich_text(props.get("Lightning transaction proof of payment", {}))
        ),
        # Stage 5 self-reported amounts (for candidate attribution)
        "stage5_onchain_sats": _get_rich_text(props.get("How many Satoshis did you send on-chain?", {})),
        "stage5_lightning_sats": _get_rich_text(props.get("How many Satoshis did you send via Lightning?", {})),
        # Deadline extension (manual override of the 7-day timeout window)
        "extended_deadline": _get_date(props.get("Extended Deadline", {})),
    }


def update_candidate(page_id: str, properties: dict) -> dict:
    """Update a candidate's properties in Notion."""
    notion_props = {}
    for key, value in properties.items():
        if value is None:
            continue
        if key in ("AI Score Stage 1", "Stage 2 Score", "Stage 3 Score", "Stage 4 Score", "Stage 5 Score", "Final Score"):
            notion_props[key] = {"number": value}
        elif key in ("AI Decision Stage 1", "Stage", "Stage 5 Result", "Email Action"):
            notion_props[key] = {"select": {"name": value}}
        elif key in ("AI Reasoning", "Strengths", "Weaknesses", "Red Flags"):
            text = value if isinstance(value, str) else ", ".join(value) if isinstance(value, list) else str(value)
            # Notion rich_text has a 2000 char limit per text block
            notion_props[key] = {"rich_text": _make_rich_text_blocks(text)}
    return _request("PATCH", f"/pages/{page_id}", {"properties": notion_props})


def advance_candidate(page_id: str, next_stage: str) -> dict:
    """Move a candidate to the next stage."""
    return update_candidate(page_id, {"Stage": next_stage})


def reject_candidate(page_id: str, reasoning: str) -> dict:
    """Reject a candidate with reasoning."""
    return update_candidate(page_id, {
        "Stage": "Rejected",
        "AI Reasoning": reasoning,
    })


def get_page_content(page_id: str) -> str:
    """Read all text content from a Notion page (for reading prompts)."""
    blocks = []
    has_more = True
    start_cursor = None

    while has_more:
        path = f"/blocks/{page_id}/children?page_size=100"
        if start_cursor:
            path += f"&start_cursor={start_cursor}"
        data = _request("GET", path)
        blocks.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        if has_more:
            time.sleep(0.35)

    text_parts = []
    for block in blocks:
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        if "rich_text" in block_data:
            text = "".join(t.get("plain_text", "") for t in block_data["rich_text"])
            if text.strip():
                text_parts.append(text)
    return "\n".join(text_parts)


def get_code_blocks(page_id: str) -> list[str]:
    """Extract code block contents from a Notion page (for reading AI prompts)."""
    blocks = []
    has_more = True
    start_cursor = None

    while has_more:
        path = f"/blocks/{page_id}/children?page_size=100"
        if start_cursor:
            path += f"&start_cursor={start_cursor}"
        data = _request("GET", path)
        blocks.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
        if has_more:
            time.sleep(0.35)

    code_blocks = []
    for block in blocks:
        if block.get("type") == "code":
            code_data = block.get("code", {})
            text = "".join(t.get("plain_text", "") for t in code_data.get("rich_text", []))
            if text.strip():
                code_blocks.append(text)
    return code_blocks


# --- Property extraction helpers ---

def _get_title(prop: dict) -> str:
    title_list = prop.get("title", [])
    return "".join(t.get("plain_text", "") for t in title_list)


def _get_rich_text(prop: dict) -> str:
    rt_list = prop.get("rich_text", [])
    return "".join(t.get("plain_text", "") for t in rt_list)


def _get_number(prop: dict) -> int | float | None:
    return prop.get("number")


def _get_select(prop: dict) -> str | None:
    sel = prop.get("select")
    return sel.get("name") if sel else None


def _get_files(prop: dict) -> list[str]:
    """Extract ALL file URLs from a files property."""
    files = prop.get("files", [])
    urls = []
    for f in files:
        if f.get("type") == "file":
            url = f.get("file", {}).get("url")
            if url:
                urls.append(url)
        elif f.get("type") == "external":
            url = f.get("external", {}).get("url")
            if url:
                urls.append(url)
    return urls


def _get_multi_select(prop: dict) -> list[str]:
    ms_list = prop.get("multi_select", [])
    return [item.get("name", "") for item in ms_list]


def _get_email(prop: dict) -> str | None:
    return prop.get("email")


def _get_url(prop: dict) -> str | None:
    return prop.get("url")


def _get_date(prop: dict) -> str | None:
    """Return the start date (ISO string) from a Notion date property, or None."""
    date = prop.get("date")
    return date.get("start") if date else None


def _make_rich_text_blocks(text: str) -> list[dict]:
    """Split text into chunks of 2000 chars for Notion's limit."""
    chunks = []
    for i in range(0, len(text), 2000):
        chunks.append({"type": "text", "text": {"content": text[i:i + 2000]}})
    return chunks


# --- File transfer helpers (used by the Stage 2 submission auto-merge) ---

_UPLOAD_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
}


def _guess_content_type(filename: str) -> str:
    ext = filename.lower().rsplit(".", 1)[-1]
    return {
        "png": "image/png",
        "jpg": "image/jpeg", "jpeg": "image/jpeg",
        "gif": "image/gif", "webp": "image/webp", "bmp": "image/bmp",
        "pdf": "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "doc": "application/msword",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }.get(ext, "application/octet-stream")


def transfer_file_to_notion(source_url: str, filename: str) -> dict:
    """Download a file from a URL and re-upload it to Notion's file storage.

    Returns a Notion files-property entry ready to be placed inside
    {"files": [<entry>, ...]}.
    """
    content_type = _guess_content_type(filename)

    # 1) Download the source file bytes
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        dl = client.get(source_url)
        dl.raise_for_status()
        payload = dl.content

    # 2) Create a Notion file_upload object
    with httpx.Client(timeout=30) as client:
        fu = client.post(
            f"{BASE_URL}/file_uploads",
            headers=HEADERS,
            json={"mode": "single_part", "filename": filename, "content_type": content_type},
        )
        fu.raise_for_status()
        fu_data = fu.json()

    upload_url = fu_data["upload_url"]
    fu_id = fu_data["id"]

    # 3) Send the bytes to the upload_url (multipart)
    with httpx.Client(timeout=120) as client:
        send = client.post(
            upload_url,
            headers=_UPLOAD_HEADERS,
            files={"file": (filename, payload, content_type)},
        )
        send.raise_for_status()

    return {"type": "file_upload", "file_upload": {"id": fu_id}, "name": filename}


def archive_page(page_id: str) -> dict:
    """Archive (soft-delete) a page."""
    return _request("PATCH", f"/pages/{page_id}", {"archived": True})


def patch_page_properties(page_id: str, properties: dict) -> dict:
    """Generic property patch — bypasses update_candidate's narrow type allowlist."""
    return _request("PATCH", f"/pages/{page_id}", {"properties": properties})
