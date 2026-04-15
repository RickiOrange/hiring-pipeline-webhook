"""Blockchain verification for Stage 5 Bitcoin execution test.

Verifies on-chain BTC transactions via mempool.space API and
Lightning payments via Blink (Galoy) GraphQL API.
"""

import httpx

MEMPOOL_API_BASE = "https://mempool.space/api"
BLINK_GRAPHQL_ENDPOINT = "https://api.blink.sv/graphql"
TARGET_BTC_ADDRESS = "bc1qenlchjyttl2h0txvdkqda9ssqkjgt8f926yvg3"


def verify_onchain_transaction(
    address: str = TARGET_BTC_ADDRESS,
    txid: str | None = None,
) -> dict:
    """Query mempool.space for transactions to the given BTC address.

    Args:
        address: Bitcoin address to check.
        txid: Optional transaction ID provided by the candidate.

    Returns dict with:
        verified: bool - whether any confirmed tx was found to address
        txid_match: bool | None - True if provided txid found, None if no txid given
        transactions: list of {txid, amount_sats, confirmed, confirmations, timestamp}
        total_received_sats: int
        error: str | None
    """
    result = {
        "verified": False,
        "txid_match": None,
        "transactions": [],
        "total_received_sats": 0,
        "error": None,
    }

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.get(f"{MEMPOOL_API_BASE}/address/{address}/txs")
            resp.raise_for_status()
            raw_txs = resp.json()
    except Exception as e:
        result["error"] = f"mempool.space API error: {e}"
        return result

    for tx in raw_txs:
        tx_id = tx.get("txid", "")
        status = tx.get("status", {})
        confirmed = status.get("confirmed", False)
        block_time = status.get("block_time")
        block_height = status.get("block_height")

        # Sum outputs going to the target address
        amount_sats = 0
        for vout in tx.get("vout", []):
            if vout.get("scriptpubkey_address") == address:
                amount_sats += vout.get("value", 0)

        if amount_sats > 0:
            result["transactions"].append({
                "txid": tx_id,
                "amount_sats": amount_sats,
                "confirmed": confirmed,
                "block_height": block_height,
                "timestamp": block_time,
            })
            result["total_received_sats"] += amount_sats

    if result["transactions"]:
        result["verified"] = True

    # Check candidate-provided txid
    if txid:
        txid_clean = txid.strip()
        known_txids = {t["txid"] for t in result["transactions"]}
        result["txid_match"] = txid_clean in known_txids

    return result


def match_onchain_by_amount(onchain_data: dict, claimed_sats: int, submission_timestamp: int | None = None) -> dict:
    """Try to match a specific transaction by the candidate's claimed amount.

    Args:
        onchain_data: Result from verify_onchain_transaction().
        claimed_sats: Amount in sats the candidate says they sent.
        submission_timestamp: Unix timestamp of when the form was submitted (for time proximity).

    Returns dict with:
        matched: bool
        matching_txs: list of transactions matching the claimed amount
        best_match: the closest transaction by time (if submission_timestamp given)
    """
    matches = [tx for tx in onchain_data.get("transactions", []) if tx["amount_sats"] == claimed_sats]

    best = None
    if matches and submission_timestamp:
        # Pick the transaction closest in time to the form submission
        best = min(matches, key=lambda tx: abs((tx.get("timestamp") or 0) - submission_timestamp))
    elif matches:
        best = matches[0]

    return {
        "matched": len(matches) > 0,
        "matching_txs": matches,
        "best_match": best,
    }


def match_lightning_by_amount(lightning_data: dict, claimed_sats: int, submission_timestamp: int | None = None) -> dict:
    """Try to match a specific Lightning payment by the candidate's claimed amount.

    Args:
        lightning_data: Result from verify_lightning_payment().
        claimed_sats: Amount in sats the candidate says they sent.
        submission_timestamp: Unix timestamp of when the form was submitted (for time proximity).

    Returns dict with:
        matched: bool
        matching_payments: list of payments matching the claimed amount
        best_match: the closest payment by time (if submission_timestamp given)
    """
    matches = [p for p in lightning_data.get("recent_payments", []) if p["amount_sats"] == claimed_sats]

    best = None
    if matches and submission_timestamp:
        best = min(matches, key=lambda p: abs((p.get("created_at") or 0) - submission_timestamp))
    elif matches:
        best = matches[0]

    return {
        "matched": len(matches) > 0,
        "matching_payments": matches,
        "best_match": best,
    }


def verify_lightning_payment(
    api_key: str,
    payment_hash: str | None = None,
) -> dict:
    """Query Blink GraphQL API for incoming Lightning payments.

    Args:
        api_key: Blink API key (X-API-KEY header).
        payment_hash: Optional payment hash provided by the candidate.

    Returns dict with:
        verified: bool - whether any incoming settled payment exists
        hash_match: bool | None - True if provided hash matched, None if no hash given
        recent_payments: list of {direction, status, amount_sats, created_at, payment_hash}
        error: str | None
    """
    result = {
        "verified": False,
        "hash_match": None,
        "recent_payments": [],
        "error": None,
    }

    query = """
    query GetTransactions {
        me {
            defaultAccount {
                transactions(first: 50) {
                    edges {
                        node {
                            direction
                            status
                            settlementAmount
                            createdAt
                            initiationVia {
                                ... on InitiationViaLn {
                                    paymentHash
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """

    try:
        with httpx.Client(timeout=30) as client:
            resp = client.post(
                BLINK_GRAPHQL_ENDPOINT,
                json={"query": query},
                headers={
                    "Content-Type": "application/json",
                    "X-API-KEY": api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        result["error"] = f"Blink API error: {e}"
        return result

    # Check for GraphQL errors
    if "errors" in data:
        result["error"] = f"Blink GraphQL errors: {data['errors']}"
        return result

    # Extract transactions
    try:
        edges = data["data"]["me"]["defaultAccount"]["transactions"]["edges"]
    except (KeyError, TypeError) as e:
        result["error"] = f"Unexpected Blink response structure: {e}"
        return result

    for edge in edges:
        node = edge.get("node", {})
        direction = node.get("direction", "")
        status = node.get("status", "")

        if direction == "RECEIVE" and status == "SUCCESS":
            initiation = node.get("initiationVia", {})
            p_hash = initiation.get("paymentHash") if isinstance(initiation, dict) else None

            result["recent_payments"].append({
                "direction": direction,
                "status": status,
                "amount_sats": abs(node.get("settlementAmount", 0)),
                "created_at": node.get("createdAt", ""),
                "payment_hash": p_hash,
            })

    if result["recent_payments"]:
        result["verified"] = True

    # Check candidate-provided payment hash
    if payment_hash:
        hash_clean = payment_hash.strip()
        known_hashes = {p["payment_hash"] for p in result["recent_payments"] if p["payment_hash"]}
        result["hash_match"] = hash_clean in known_hashes

    return result
