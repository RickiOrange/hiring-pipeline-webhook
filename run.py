#!/usr/bin/env python3
"""CLI entry point for the AI hiring pipeline.

Usage:
    python run.py --role head_of_sales stage1
    python run.py --role head_of_sales stage2
    python run.py --role head_of_sales stage3
    python run.py --role head_of_sales stage4
    python run.py --role head_of_sales stage5
    python run.py --role head_of_sales rank
    python run.py --role head_of_sales timeout
"""

import argparse
import sys

from pipeline import (
    load_config, run_stage1, run_stage2, run_stage3,
    run_stage4, run_stage5, run_ranking, run_timeout_check,
    run_health_check,
)


def main():
    parser = argparse.ArgumentParser(description="AI Hiring Pipeline")
    parser.add_argument(
        "--role",
        required=True,
        help="Role config name (e.g., head_of_sales). Must match a YAML file in config/",
    )
    parser.add_argument(
        "stage",
        choices=["stage1", "stage2", "stage3", "stage4", "stage5", "rank", "timeout", "health"],
        help="Pipeline stage to run",
    )
    args = parser.parse_args()

    try:
        config = load_config(args.role)
    except FileNotFoundError:
        print(f"ERROR: Config file not found: config/{args.role}.yaml")
        sys.exit(1)

    print(f"=== AI Hiring Pipeline ===")
    print(f"Role: {config['role_name']}")
    print(f"Stage: {args.stage}")
    print(f"Database: {config['notion_database_id']}")
    print()

    if args.stage == "stage1":
        run_stage1(config)
    elif args.stage == "stage2":
        run_stage2(config)
    elif args.stage == "stage3":
        run_stage3(config)
    elif args.stage == "stage4":
        run_stage4(config)
    elif args.stage == "stage5":
        run_stage5(config)
    elif args.stage == "rank":
        run_ranking(config)
    elif args.stage == "timeout":
        run_timeout_check(config)
    elif args.stage == "health":
        run_health_check(config)


if __name__ == "__main__":
    main()
