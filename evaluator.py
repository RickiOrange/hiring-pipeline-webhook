"""Generic AI evaluator using Claude API for candidate assessment."""

import json
import os
import re

import anthropic
import httpx
from dotenv import load_dotenv

load_dotenv(override=True)

CLIENT = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
DEFAULT_MODEL = "claude-sonnet-4-20250514"


def evaluate_candidate(
    candidate_data: dict,
    prompt_template: str,
    submission_text: str = "",
    cv_content: str = "",
    model: str = DEFAULT_MODEL,
) -> dict:
    """Send candidate data to Claude for evaluation. Returns parsed JSON result."""
    # Fill in template placeholders
    prompt = prompt_template
    prompt = prompt.replace("{candidate_data}", _format_candidate_data(candidate_data))
    prompt = prompt.replace("{cv_content}", cv_content or "No CV provided")
    prompt = prompt.replace("{submission_text}", submission_text or "No submission provided")

    message = CLIENT.messages.create(
        model=model,
        max_tokens=2000,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text
    return _parse_json_response(response_text)


def evaluate_ranking(
    candidates: list[dict],
    prompt_template: str,
    model: str = DEFAULT_MODEL,
) -> dict:
    """Generate a ranked shortlist of candidates."""
    candidates_json = json.dumps(candidates, indent=2, default=str)
    prompt = prompt_template.replace("{candidates_json}", candidates_json)

    message = CLIENT.messages.create(
        model=model,
        max_tokens=4000,
        temperature=0,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text
    return _parse_json_response(response_text)


def evaluate_with_images(
    prompt: str,
    image_urls: list[str],
    text_content: str = "",
    model: str = DEFAULT_MODEL,
) -> dict:
    """Send a prompt with images to Claude for visual evaluation. Returns parsed JSON."""
    content = []

    # Add images first
    for url in image_urls:
        if not url:
            continue
        try:
            image_data = _download_image(url)
            if image_data:
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": image_data["media_type"],
                        "data": image_data["data"],
                    },
                })
        except Exception as e:
            content.append({"type": "text", "text": f"[Failed to load image: {e}]"})

    # Add the text prompt
    full_prompt = prompt
    if text_content:
        full_prompt += f"\n\nADDITIONAL TEXT CONTENT:\n{text_content}"
    content.append({"type": "text", "text": full_prompt})

    if not content:
        return {"score": 0, "decision": "Fail", "reasoning": "No content to evaluate"}

    message = CLIENT.messages.create(
        model=model,
        max_tokens=3000,
        temperature=0,
        messages=[{"role": "user", "content": content}],
    )

    response_text = message.content[0].text
    return _parse_json_response(response_text)


def _download_image(url: str) -> dict | None:
    """Download an image from URL and return base64 data with media type."""
    import base64
    if not url:
        return None
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "image/png")
        # Normalize media type
        if "jpeg" in content_type or "jpg" in content_type:
            media_type = "image/jpeg"
        elif "png" in content_type:
            media_type = "image/png"
        elif "gif" in content_type:
            media_type = "image/gif"
        elif "webp" in content_type:
            media_type = "image/webp"
        else:
            media_type = "image/png"  # default
        data = base64.standard_b64encode(resp.content).decode("utf-8")
        return {"media_type": media_type, "data": data}


def fetch_all_file_urls(files_value: str | None) -> list[str]:
    """Extract all file URLs from a Notion files property value.
    The _get_files helper returns the first URL, but we may need all of them."""
    # For now this returns a single-item list from the first URL
    if files_value:
        return [files_value]
    return []


def fetch_cv_content(url: str) -> str:
    """Attempt to fetch CV text from a URL. Returns empty string on failure."""
    if not url:
        return ""
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get(url)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "text" in content_type or "html" in content_type:
                return resp.text[:10000]  # Limit to 10k chars
            return f"[Binary file at {url} - cannot extract text. Content-Type: {content_type}]"
    except Exception as e:
        return f"[Could not fetch CV from {url}: {e}]"


def _format_candidate_data(data: dict) -> str:
    """Format candidate data dict into a readable string for the prompt."""
    lines = []
    for key, value in data.items():
        if key == "page_id":
            continue
        label = key.replace("_", " ").title()
        lines.append(f"- {label}: {value if value is not None else 'Not provided'}")
    return "\n".join(lines)


def _parse_json_response(text: str) -> dict:
    """Extract and parse JSON from Claude's response."""
    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON in the response
    json_match = re.search(r"\{[\s\S]*\}", text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {
        "score": 0,
        "decision": "Manual Review",
        "reasoning": f"Failed to parse AI response. Raw output: {text[:500]}",
        "strengths": [],
        "weaknesses": [],
        "red_flags": ["AI evaluation parse error"],
    }
