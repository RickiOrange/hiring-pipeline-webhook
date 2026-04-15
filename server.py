"""FastAPI webhook server for real-time candidate processing.

Receives webhook calls from Notion automations when a new candidate
submits the application form, and triggers Stage 1 evaluation immediately.

Usage (local):
    uvicorn server:app --reload

Environment variables:
    ROLE             — role config to load (default: head_of_sales)
    WEBHOOK_SECRET   — shared secret for authenticating webhook calls
    NOTION_API_KEY   — Notion integration token
    ANTHROPIC_API_KEY — Claude API key
"""

import asyncio
import hmac
import logging
import os

from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Header, Request

from pipeline import load_config, process_single_stage1

load_dotenv(override=True)

logger = logging.getLogger("hiring-webhook")
logging.basicConfig(level=logging.INFO)

# --- Config (loaded once at startup) ---

CONFIG: dict = {}
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load pipeline config once when the server starts."""
    role = os.getenv("ROLE", "head_of_sales")
    CONFIG.update(load_config(role))
    logger.info(f"Loaded config for role: {CONFIG.get('role_name', role)}")
    yield


app = FastAPI(title="Hiring Pipeline Webhook", lifespan=lifespan)


# --- Auth ---

def verify_secret(provided: str | None):
    """Validate the webhook secret using constant-time comparison."""
    if not WEBHOOK_SECRET:
        return  # No secret configured — allow all (dev mode)
    if not provided or not hmac.compare_digest(provided, WEBHOOK_SECRET):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")


# --- Endpoints ---

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "role": CONFIG.get("role_name", "not loaded"),
    }


def _extract_page_id(body: dict) -> str | None:
    """Extract page_id from various webhook payload formats.

    Supports:
      - {"page_id": "abc123"}                         (direct)
      - {"data": {"page_id": "abc123"}}               (nested)
      - {"id": "abc123-..."}                           (Notion native)
      - {"data": [{"id": "abc123", ...}]}              (Notion automation array)
      - {"properties": {...}, "id": "..."}             (Notion page object)
    """
    # Direct page_id field
    if body.get("page_id"):
        return body["page_id"]

    # Nested under data dict
    data = body.get("data")
    if isinstance(data, dict) and data.get("page_id"):
        return data["page_id"]

    # Notion native: top-level id field
    if body.get("id"):
        return body["id"]

    # Notion automation sends array of page objects
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        if isinstance(first, dict) and first.get("id"):
            return first["id"]

    # Nested under data dict: id field
    if isinstance(data, dict) and data.get("id"):
        return data["id"]

    return None


@app.post("/webhook/stage1")
async def webhook_stage1(
    request: Request,
    x_webhook_secret: str | None = Header(None),
):
    """Process a single candidate through Stage 1.

    Accepts Notion automation webhooks and direct POST requests.
    Flexibly extracts page_id from multiple payload formats.
    """
    verify_secret(x_webhook_secret)

    body = await request.json()
    logger.info(f"Webhook payload received: {body}")

    page_id = _extract_page_id(body)
    if not page_id:
        logger.error(f"Could not extract page_id from payload: {body}")
        raise HTTPException(status_code=400, detail="No page_id found in payload")

    page_id = page_id.strip()
    logger.info(f"Processing page_id: {page_id}")

    # Run the sync pipeline code in a thread to avoid blocking the event loop
    try:
        result = await asyncio.to_thread(process_single_stage1, page_id, CONFIG)
    except Exception as e:
        logger.error(f"Pipeline error for {page_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Pipeline error: {str(e)}")

    logger.info(f"Result for {result.get('name', 'unknown')}: {result.get('decision')} ({result.get('score')})")

    return {"status": "ok", "result": result}
