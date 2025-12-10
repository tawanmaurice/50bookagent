import os
import json
import uuid
import re
import logging
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables from Lambda / Terraform
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

# e.g. "sga-agent", "orientation-agent", etc.
AGENT_SOURCE = os.getenv("AGENT_SOURCE", "sga-agent")

# Multiple queries separated by ||
DEFAULT_QUERY = "Student Government Association leadership retreat site:.edu"
SEARCH_QUERIES = os.getenv("SEARCH_QUERIES", DEFAULT_QUERY).split("||")

# Limit how many search results per run
MAX_RESULTS = int(os.getenv("MAX_RESULTS", "10"))

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(DDB_TABLE_NAME)


def google_search(query: str) -> list:
    """Call Google Programmable Search (CSE) for a query."""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CX,
        "q": query,
    }
    logger.info(f"[google_search] Querying Google CSE: {query}")
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("items", [])


def fetch_emails_from_url(url: str) -> list:
    """Fetch a page and extract emails using regex."""
    logger.info(f"[fetch_emails_from_url] Fetching {url}")
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"[fetch_emails_from_url] Error fetching {url}: {e}")
        return []

    # Basic email regex
    email_pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    emails = re.findall(email_pattern, html)
    unique_emails = sorted(set(emails))
    logger.info(
        f"[fetch_emails_from_url] Found {len(unique_emails)} emails on {url}"
    )
    return unique_emails


def choose_primary_email(url: str, emails: list) -> str | None:
    """Prefer .edu email matching the site's domain; else first .edu; else first."""
    if not emails:
        return None

    # Get domain from URL
    domain_match = re.search(r"https?://([^/]+)/?", url)
    page_domain = domain_match.group(1).lower() if domain_match else ""

    # 1) .edu and contains domain
    for e in emails:
        lower = e.lower()
        if lower.endswith(".edu") and page_domain and page_domain.split(":")[0] in lower:
            return e

    # 2) any .edu email
    for e in emails:
        if e.lower().endswith(".edu"):
            return e

    # 3) fallback: first email
    return emails[0]


def save_item_to_dynamodb(item: dict):
    logger.info(f"[{AGENT_SOURCE}] Saving item to DynamoDB: {item}")
    table.put_item(Item=item)


def run_agent() -> int:
    saved_count = 0
    now_iso = datetime.now(timezone.utc).isoformat()

    for query in SEARCH_QUERIES:
        query = query.strip()
        if not query:
            continue

        items = google_search(query)
        if not items:
            continue

        for result in items[:MAX_RESULTS]:
            url = result.get("link")
            title = result.get("title", "")
            snippet = result.get("snippet", "")

            if not url:
                continue

            emails = fetch_emails_from_url(url)
            primary_email = choose_primary_email(url, emails)

            # Deterministic ID based on source + URL
            record_id = uuid.uuid5(
                uuid.NAMESPACE_URL, f"{AGENT_SOURCE}:{url}"
            ).hex

            ddb_item = {
                "id": record_id,
                "url": url,
                "title": title,
                "snippet": snippet,
                "source": AGENT_SOURCE,
                "query": query,
                "timestamp": now_iso,
            }

            if primary_email:
                ddb_item["email"] = primary_email
            if emails:
                ddb_item["emails"] = emails

            save_item_to_dynamodb(ddb_item)
            saved_count += 1

    return saved_count


def lambda_handler(event, context):
    logger.info(f"[lambda_handler] Starting agent: {AGENT_SOURCE}")
    try:
        saved = run_agent()
        body = {
            "message": f"{AGENT_SOURCE} ran successfully. Saved {saved} items.",
            "saved": saved,
        }
        logger.info(body)
        return {
            "statusCode": 200,
            "body": json.dumps(body),
        }
    except Exception as e:
        logger.exception("[lambda_handler] Error running agent")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }
