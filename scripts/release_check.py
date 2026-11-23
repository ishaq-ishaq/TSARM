#!/usr/bin/env python3
"""Pre-release checks for a Zenodo-archived TSARM release (see docs/release.md).

    python3 scripts/release_check.py 0.9.0

Checks that the version agrees in setup.cfg, CITATION.cff and .zenodo.json,
that .zenodo.json is valid, that no private file is tracked by git (Word
documents, proposal amendments, thesis build files, raw data, tool jars), and
that the working tree is clean. Exit status 1 if any check fails.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PRIVATE = (
    r"\.docx?$", r"proposal_amendments", r"^thesis_build/", r"^data/raw/.+\.(bz2|gz|owl|ttl|tsv|json)$", r"^data/processed/.+\.parquet",
    r"^tools/.+\.(jar|zip)$", r"^CLAUDE\.md$",
)


def versions() -> dict:
    out = {}
    m = re.search(r"^version\s*=\s*(\S+)", (ROOT / "setup.cfg").read_text(), re.M)
    out["setup.cfg"] = m.group(1) if m else None
    m = re.search(r'^version:\s*"?([^"\n]+)"?', (ROOT / "CITATION.cff").read_text(), re.M)
    out["CITATION.cff"] = m.group(1).strip() if m else None
    try:
        out[".zenodo.json"] = json.loads((ROOT / ".zenodo.json").read_text()).get("version")
    except (OSError, json.JSONDecodeError) as exc:
        out[".zenodo.json"] = f"invalid ({exc})"
    return out


def tracked_private(files) -> list:
    return [f for f in files if any(re.search(p, f) for p in PRIVATE)]


def main() -> int:
    want = sys.argv[1] if len(sys.argv) > 1 else None
    failures = []
    vs = versions()
    if want is None:
        want = vs["setup.cfg"]
    for f, v in vs.items():
        status = "ok" if v == want else "MISMATCH"
        if v != want:
            failures.append(f"{f} has version {v!r}, expected {want!r}")
        print(f"  {f:14s} {v}  {status}")
    files = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.split()
    bad = tracked_private(files)
    if bad:
        failures.append("private files are tracked: " + ", ".join(bad))
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    if dirty:
        failures.append("working tree is not clean")
    branch = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    if branch != "main":
        failures.append(f"on branch {branch!r}; release from main")
    for f in failures:
        print("FAIL:", f)
    print("release check:", "FAILED" if failures else f"OK for v{want}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
