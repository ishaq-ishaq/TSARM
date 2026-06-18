"""Pytest setup shared by all tests.

PySpark 4.x requires Java 17+. On this machine Java is provided by Homebrew and
is not on the default ``java_home`` search path, so we point ``JAVA_HOME`` at a
suitable JDK before any SparkSession is created. We only set it when it is not
already configured, so an explicitly chosen JDK in the environment wins.
"""

import os
from pathlib import Path

# Preferred Homebrew JDKs, newest first. Spark 4.x needs >= 17.
_CANDIDATE_JDKS = [
    "/opt/homebrew/opt/openjdk@17",
    "/opt/homebrew/opt/openjdk@21",
    "/opt/homebrew/opt/openjdk",
]


def _ensure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in _CANDIDATE_JDKS:
        if (Path(candidate) / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
            return


_ensure_java_home()
