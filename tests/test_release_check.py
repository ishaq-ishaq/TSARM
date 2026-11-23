"""The release metadata stays consistent (cheap guard, run in CI)."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("release_check", ROOT / "scripts" / "release_check.py")
rc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rc)


def test_versions_agree():
    vs = rc.versions()
    assert len(set(vs.values())) == 1, vs


def test_private_patterns():
    assert rc.tracked_private(["Proposal_x.docx", "docs/proposal_amendments.md", "thesis_build/ch5.py",
                               "data/raw/x.ttl.bz2", "tools/amie/amie.jar", "CLAUDE.md"]) == [
        "Proposal_x.docx", "docs/proposal_amendments.md", "thesis_build/ch5.py",
        "data/raw/x.ttl.bz2", "tools/amie/amie.jar", "CLAUDE.md"]
    assert rc.tracked_private(["src/mining/arm.py", "data/raw/.gitkeep", "docs/release.md"]) == []
