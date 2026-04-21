"""One-off: find every Candidate Applications row that is currently marked
Rejected but actually carries a stage 2/3/4/5 form submission payload, and
merge it into the candidate's original row using the deployed pipeline logic.

These are casualties of the Railway deployment gap — orphan submissions that
hit the webhook BEFORE the auto-merge fix shipped, so Stage 1 hard filters
ran on them and rejected them with "Hard filter: No university degree".

Idempotent: rows already merged/archived are skipped automatically.
"""
import os
import sys
from typing import Optional

from dotenv import load_dotenv

load_dotenv(override=True)

# Reuse the deployed pipeline logic so this script is the same merge that
# the webhook would now perform on a fresh submission.
from pipeline import (
    STAGE_SUBMISSION_SPECS,
    _detect_stage_submission,
    _find_original_candidate,
    _merge_stage_submission,
)
from notion_client import (
    get_candidates,
    get_candidate_data,
    get_page,
)


DB_ID = "336eddaa-d74c-8188-b616-c32aa4da025e"


def main() -> None:
    print("Scanning for Rejected rows that carry stage-submission payload...\n")
    all_rows = get_candidates(DB_ID)
    rejected = [
        r for r in all_rows
        if not r.get("archived") and not r.get("in_trash")
        and ((r.get("properties", {}).get("Stage") or {}).get("select") or {}).get("name") == "Rejected"
    ]
    print(f"  Found {len(rejected)} non-archived Rejected rows total.\n")

    suspects = []
    for row in rejected:
        candidate = get_candidate_data(row)
        # Provisional spec — same call the webhook makes
        spec = _detect_stage_submission(candidate, row.get("properties", {}))
        if spec:
            suspects.append((row, candidate, spec))

    if not suspects:
        print("No orphan victims found. All Rejected rows are legitimate Stage 1 rejections.")
        return

    print(f"Found {len(suspects)} suspected orphan victim(s):\n")
    for row, cand, spec in suspects:
        name = cand.get("full_name") or "?"
        email = cand.get("email") or "?"
        print(f"  - {spec['label']} | {name} | email={email} | id={row['id']}")
    print()

    merged = 0
    skipped = 0
    failed = 0
    for row, cand, provisional_spec in suspects:
        name = cand.get("full_name") or "?"
        email = cand.get("email") or ""
        print(f"\n=== {provisional_spec['label']} orphan: {name} (id={row['id']}) ===")
        try:
            # Find original — name primary, email disambiguator
            original = _find_original_candidate(name, email, DB_ID, exclude_page_id=row["id"])
            if not original:
                print(f"  [unmatched] no original candidate row found for {name!r} / {email!r}")
                skipped += 1
                continue

            # Re-detect with original's current Stage so Stage 2 vs Stage 3
            # disambiguation works (matches the webhook behaviour exactly).
            original_stage = (
                (original.get("properties", {}).get("Stage") or {}).get("select") or {}
            ).get("name")
            refined = _detect_stage_submission(
                cand, row.get("properties", {}), current_stage=original_stage,
            )
            spec = refined or provisional_spec
            if refined and refined["label"] != provisional_spec["label"]:
                print(f"  [disambiguated {provisional_spec['label']} -> {spec['label']}] "
                      f"original is at {original_stage!r}")

            print(f"  Merging into original {original['id']} (stage={original_stage!r})")
            # Re-fetch the orphan to get fresh signed file URLs
            orphan_fresh = get_page(row["id"])
            result = _merge_stage_submission(orphan_fresh, original, spec)
            print(f"  Merge result: merged={result.get('merged')} "
                  f"fields={result.get('fields_merged')} "
                  f"overwritten={result.get('fields_overwritten')} "
                  f"blocked={result.get('blocked_reason')}")
            if result.get("merged"):
                merged += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            failed += 1

    print(f"\n--- Sweep complete ---")
    print(f"Merged:  {merged}")
    print(f"Skipped: {skipped}")
    print(f"Failed:  {failed}")


if __name__ == "__main__":
    main()
