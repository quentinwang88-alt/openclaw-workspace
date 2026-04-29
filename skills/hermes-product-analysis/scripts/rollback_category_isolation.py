#!/usr/bin/env python3
"""Rollback placeholder for category-isolation migration artifacts."""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", required=False, default="")
    args = parser.parse_args()
    print("Rollback requires an explicit artifact/database backup. No destructive action was taken. backup={backup}".format(backup=args.backup or ""))


if __name__ == "__main__":
    main()
