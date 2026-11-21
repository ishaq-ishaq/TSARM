"""Write the ranked temporal rule set to JSON and CSV (proposal Sec. 3.6.5).

The output of the temporal evaluation layer is "a sorted collection of annotated
temporal rules, each carrying its lifecycle classification, the three temporal
metric scores, and the full per-window confidence trajectory", written as JSON
and CSV. The final rule set is small relative to the input graph (thousands of
rules, not millions of triples), so it is collected to the driver and written
as single human-readable files.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame

CSV_LIST_SEP = ";"


def _clean(value):
    """Make a Spark value JSON-safe (NaN/inf -> None, Row -> dict)."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if hasattr(value, "asDict"):
        return {k: _clean(v) for k, v in value.asDict().items()}
    return value


def export_rules(
    metrics: DataFrame,
    out_dir,
    basename: str = "temporal_rules",
    params: Dict = None,
) -> Tuple[Path, Path]:
    """Write ``metrics`` (from :func:`compute_metrics`) as JSON and CSV.

    Row order is preserved, so the files carry the ranking. The JSON file is
    ``{"params": {...}, "n_rules": N, "rules": [ {...}, ... ]}`` with list
    columns as JSON arrays; in the CSV, list columns are joined with ``;``.

    Args:
        metrics: Ranked rule metrics.
        out_dir: Destination directory (created if missing).
        basename: File stem; writes ``<basename>.json`` and ``<basename>.csv``.
        params: Run parameters (thresholds, window config, dataset) recorded in
            the JSON header for reproducibility.

    Returns:
        ``(json_path, csv_path)``.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    columns = metrics.columns
    rows: List[Dict] = [
        {c: _clean(r[c]) for c in columns} for r in metrics.collect()
    ]

    json_path = out / f"{basename}.json"
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(
            {"params": params or {}, "n_rules": len(rows), "rules": rows},
            fh,
            indent=2,
            ensure_ascii=False,
        )

    csv_path = out / f"{basename}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    k: CSV_LIST_SEP.join("" if x is None else str(x) for x in v)
                    if isinstance(v, list)
                    else v
                    for k, v in row.items()
                }
            )
    return json_path, csv_path
