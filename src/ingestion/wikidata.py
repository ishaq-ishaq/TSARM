"""Streaming extraction of class statements (+ temporal qualifiers) from Wikidata dumps.

Layer 1 of the proposal architecture for Wikidata (§3.6.2). Historical Wikidata
snapshots survive in two formats (see docs/wikidata_data_plan.md):

* **Entity JSON dumps** (``*-all.json.gz/bz2``): one entity JSON object per
  line, wrapped in ``[`` ... ``]`` with trailing commas.
* **Page XML dumps** (``*-pages-articles.xml.bz2``): MediaWiki XML whose item
  pages (namespace 0) carry the entity JSON as page text.

Both are streamed in a single pass. Only the statements TSARM needs are kept,
by default **P31** (instance of) and **P279** (subclass of), the class
assertions behind semantic item generation (Algorithm 3.1). Each kept statement
records:

* its **rank**, and whether it is **best-rank**, i.e. what Wikidata's "truthy"
  view would expose: preferred statements if the (entity, property) pair has
  any, otherwise normal ones; deprecated statements are never best-rank;
* its **P580 / P582 / P585** (start / end / point in time) qualifiers,
  normalised to ISO-8601 dates plus a Wikidata precision code. These give the
  *valid-time* dimension (§2.3.3), alongside the snapshot (transaction) time of
  the dump itself.

Output is a gzip TSV with a header row; :func:`read_statements` loads it into
Spark in TSARM's canonical triple schema with extra temporal columns. The raw
dump never needs to be stored: pipe ``curl ... | lbzip2 -dc`` into the
extractor (``scripts/extract_wikidata.py``) and only the small filtered file
reaches disk.
"""

from __future__ import annotations

import gzip
import io
import json
import re
import xml.etree.ElementTree as ET
from typing import IO, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

ENTITY_NS = "http://www.wikidata.org/entity/"
DIRECT_NS = "http://www.wikidata.org/prop/direct/"

DEFAULT_PROPERTIES = ("P31", "P279")
START, END, POINT = "P580", "P582", "P585"

COLUMNS = (
    "subject",
    "predicate",
    "object",
    "rank",
    "best_rank",
    "start_time",
    "start_precision",
    "end_time",
    "end_precision",
    "point_in_time",
    "point_precision",
)

Row = Tuple[str, str, str, str, bool, Optional[str], Optional[int], Optional[str], Optional[int], Optional[str], Optional[int]]

_TIME_RE = re.compile(r"^([+-])(\d+)-(\d{2})-(\d{2})T")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def normalize_time(value: Dict) -> Tuple[Optional[str], Optional[int]]:
    """Wikidata time value -> (ISO date string, precision).

    ``+2001-00-00T00:00:00Z`` (precision 9 = year) becomes ``2001-01-01``:
    unknown month/day (``00``) are set to 01. Years are zero-padded to 4 digits
    and BCE years keep a leading ``-``. Precision codes follow Wikidata (9 =
    year, 10 = month, 11 = day; lower = decade, century, ...).
    """
    if not isinstance(value, dict):
        return None, None
    raw = value.get("time")
    m = _TIME_RE.match(raw or "")
    if not m:
        return None, None
    sign, year, month, day = m.groups()
    month = month if month != "00" else "01"
    day = day if day != "00" else "01"
    iso = f"{'-' if sign == '-' else ''}{int(year):04d}-{month}-{day}"
    precision = value.get("precision")
    return iso, int(precision) if isinstance(precision, int) else None


def _item_id(datavalue: Dict) -> Optional[str]:
    """Q-id from a wikibase-entityid value (old dumps lack the ``id`` key)."""
    value = (datavalue or {}).get("value")
    if not isinstance(value, dict):
        return None
    if value.get("id"):
        return value["id"]
    if value.get("entity-type", "item") == "item" and "numeric-id" in value:
        return f"Q{value['numeric-id']}"
    return None


def _first_time(qualifiers: Dict, prop: str) -> Tuple[Optional[str], Optional[int]]:
    for snak in (qualifiers or {}).get(prop, []) or []:
        if snak.get("snaktype") == "value":
            iso, prec = normalize_time((snak.get("datavalue") or {}).get("value"))
            if iso:
                return iso, prec
    return None, None


def extract_entity(entity: Dict, properties: Sequence[str] = DEFAULT_PROPERTIES) -> List[Row]:
    """Kept statements of one entity JSON object, as output rows."""
    if not isinstance(entity, dict) or entity.get("type", "item") != "item":
        return []
    subject_id = entity.get("id")
    claims = entity.get("claims")
    if not subject_id or not isinstance(claims, dict):
        return []
    rows: List[Row] = []
    for prop in properties:
        statements = claims.get(prop) or []
        if not isinstance(statements, list):
            continue
        has_preferred = any(s.get("rank") == "preferred" for s in statements)
        for st in statements:
            snak = st.get("mainsnak") or {}
            if snak.get("snaktype") != "value":
                continue  # somevalue / novalue
            obj = _item_id(snak.get("datavalue"))
            if not obj:
                continue
            rank = st.get("rank", "normal")
            best = rank == "preferred" or (rank == "normal" and not has_preferred)
            quals = st.get("qualifiers") or {}
            start, start_p = _first_time(quals, START)
            end, end_p = _first_time(quals, END)
            point, point_p = _first_time(quals, POINT)
            rows.append(
                (
                    ENTITY_NS + subject_id,
                    DIRECT_NS + prop,
                    ENTITY_NS + obj,
                    rank,
                    best,
                    start,
                    start_p,
                    end,
                    end_p,
                    point,
                    point_p,
                )
            )
    return rows


def parse_entity_text(text: str, properties: Sequence[str] = DEFAULT_PROPERTIES) -> List[Row]:
    """Parse one entity's JSON text; malformed text yields no rows."""
    text = text.strip().rstrip(",")
    if not text.startswith("{"):
        return []
    try:
        return extract_entity(json.loads(text), properties)
    except (ValueError, TypeError, AttributeError):
        return []


# ---------------------------------------------------------------------------
# Streaming readers: yield one entity JSON text at a time
# ---------------------------------------------------------------------------


def iter_json_dump(stream: IO[str]) -> Iterator[str]:
    """Entity texts from a JSON dump (one entity per line inside ``[ ... ]``)."""
    for line in stream:
        line = line.strip()
        if line.startswith("{"):
            yield line


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


_XML_REF = re.compile(r"&(#x[0-9a-fA-F]+|#[0-9]+|quot|lt|gt|amp|apos);")
_NAMED = {"quot": '"', "lt": "<", "gt": ">", "amp": "&", "apos": "'"}
_READ_SIZE = 1 << 23


def _xml_ref(m: "re.Match[str]") -> str:
    ref = m.group(1)
    if ref[0] != "#":
        return _NAMED[ref]
    return chr(int(ref[2:], 16) if ref[1] == "x" else int(ref[1:]))


def _xml_text(raw: bytes) -> str:
    """Decode XML character data: entities, character references, line ends."""
    if b"\r" in raw:  # XML end-of-line handling, as an XML parser applies it
        raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    text = raw.decode("utf-8")
    if "&" not in text:
        return text
    if "&#" in text:  # rare: one exact single pass
        return _XML_REF.sub(_xml_ref, text)
    # Fast path; "&amp;" last, so an escaped "&amp;lt;" correctly stays "&lt;".
    text = text.replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">").replace("&apos;", "'")
    return text.replace("&amp;", "&")


def xml_text(raw: bytes) -> Optional[str]:
    """Entity text from a page's raw ``<text>`` content, or None if not JSON.

    Decodes the XML escaping (see :func:`iter_xml_raw`)."""
    text = _xml_text(raw)
    return text if text.lstrip().startswith("{") else None


def _page_text(page: bytes) -> Optional[bytes]:
    """Raw (still XML-escaped) text of one <page> block if it is an item page."""
    i = page.find(b"<ns>")
    if i < 0:
        return None
    j = page.find(b"</ns>", i)
    if page[i + 4 : j].strip() != b"0":
        return None
    t = page.rfind(b"<text")  # the last revision's text, as an XML parser would give
    if t < 0:
        return None
    gt = page.find(b">", t)
    if gt < 0 or page[gt - 1 : gt] == b"/":  # <text ... /> : no content
        return None
    end = page.find(b"</text>", gt)
    return None if end < 0 else page[gt + 1 : end]


def iter_xml_raw(stream: IO[bytes]) -> Iterator[bytes]:
    """Raw ``<text>`` content of the item pages (namespace 0) of a pages XML dump.

    Scans the byte stream for ``<page>`` blocks instead of building an XML tree.
    This is exact for MediaWiki dumps because element content is escaped: a
    literal ``<`` occurs only in markup, so tags can be located by byte search.
    The content is returned still escaped; :func:`xml_text` decodes it, so the
    extractor can do that in its worker processes and keep the single reading
    thread fast (about 3 times the speed of :func:`iter_xml_dump_etree`).
    """
    buf = b""
    while True:
        chunk = stream.read(_READ_SIZE)
        if chunk:
            buf += chunk
        pos = 0
        while True:
            start = buf.find(b"<page>", pos)
            if start < 0:
                break
            end = buf.find(b"</page>", start)
            if end < 0:
                break
            raw = _page_text(buf[start:end])
            if raw is not None:
                yield raw
            pos = end + 7
        buf = buf[pos:]
        if not chunk:
            return


def iter_xml_dump(stream: IO[bytes]) -> Iterator[str]:
    """Entity texts from a MediaWiki pages XML dump (item pages, namespace 0).

    Same output as :func:`iter_xml_dump_etree` (checked in
    ``tests/test_wikidata.py``), via the byte scanner :func:`iter_xml_raw`.
    """
    for raw in iter_xml_raw(stream):
        text = xml_text(raw)
        if text is not None:
            yield text


def iter_xml_dump_etree(stream: IO[bytes]) -> Iterator[str]:
    """Reference reader for :func:`iter_xml_dump` using a full XML parser.

    Same output, about 4-5 times slower: the single parsing thread limits the
    whole extraction. Kept to check the fast reader against.
    """
    context = ET.iterparse(stream, events=("start", "end"))
    root = None
    for event, elem in context:
        if root is None and event == "start":
            root = elem
            continue
        if event != "end" or _local(elem.tag) != "page":
            continue
        ns = text = None
        for child in elem:
            name = _local(child.tag)
            if name == "ns":
                ns = (child.text or "").strip()
            elif name == "revision":
                for rev_child in child:
                    if _local(rev_child.tag) == "text":
                        text = rev_child.text
        if ns == "0" and text and text.lstrip().startswith("{"):
            yield text
        # Drop processed pages so memory stays flat on 100+ GB dumps.
        if root is not None:
            root.clear()


def open_dump(path: str) -> IO[bytes]:
    """Binary stream for a dump path; ``-`` is stdin; .gz/.bz2 decompressed."""
    import sys

    if path == "-":
        return sys.stdin.buffer
    if path.endswith(".gz"):
        return gzip.open(path, "rb")
    if path.endswith(".bz2"):
        import bz2

        return bz2.open(path, "rb")
    return open(path, "rb")


def iter_entities(stream: IO[bytes], fmt: str) -> Iterator[str]:
    if fmt == "json":
        return iter_json_dump(io.TextIOWrapper(stream, encoding="utf-8"))
    if fmt == "xml":
        return iter_xml_dump(stream)
    raise ValueError(f"Unknown dump format: {fmt!r} (expected 'json' or 'xml').")


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def _cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def write_rows(rows: Iterable[Row], out: IO[str]) -> int:
    """Write TSV rows (no header); returns the count."""
    n = 0
    for row in rows:
        out.write("\t".join(_cell(v) for v in row) + "\n")
        n += 1
    return n


def header() -> str:
    return "\t".join(COLUMNS) + "\n"


# ---------------------------------------------------------------------------
# Spark loader
# ---------------------------------------------------------------------------


def read_statements(spark, path, snapshot: str, snapshot_ts):
    """Load an extracted TSV into TSARM's canonical schema + temporal columns.

    Returns the canonical columns (``subject, predicate, object, object_kind,
    datatype, language, snapshot, snapshot_ts``) plus ``rank``, ``best_rank``,
    ``valid_from`` / ``valid_to`` (dates from P580 / P582, or P585 for both
    when only a point in time is given) and their precisions. Dates outside
    Spark's range (e.g. BCE) become null; the raw strings stay available in
    ``start_time`` / ``end_time`` / ``point_in_time``.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    schema = T.StructType(
        [T.StructField(c, T.StringType(), True) for c in COLUMNS]
    )
    df = spark.read.csv(str(path), sep="\t", header=True, schema=schema)

    def as_date(col):
        # Only plain 4-digit CE years parse; everything else -> null.
        return F.when(
            F.col(col).rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col(col), "yyyy-MM-dd")
        )

    start = as_date("start_time")
    end = as_date("end_time")
    point = as_date("point_in_time")
    return df.select(
        "subject",
        "predicate",
        "object",
        F.lit("iri").alias("object_kind"),
        F.lit(None).cast("string").alias("datatype"),
        F.lit(None).cast("string").alias("language"),
        F.lit(snapshot).alias("snapshot"),
        F.lit(snapshot_ts).cast(T.TimestampType()).alias("snapshot_ts"),
        "rank",
        (F.col("best_rank") == "true").alias("best_rank"),
        F.coalesce(start, point).alias("valid_from"),
        F.coalesce(end, point).alias("valid_to"),
        F.col("start_precision").cast("int").alias("start_precision"),
        F.col("end_precision").cast("int").alias("end_precision"),
        F.col("point_precision").cast("int").alias("point_precision"),
        "start_time",
        "end_time",
        "point_in_time",
    )


def snapshot_ts_from_name(name: str):
    """``2017-01`` or ``2017-01-20`` (file stem) -> datetime of that day (1st if no day)."""
    from datetime import datetime

    stem = name.split(".")[0]
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(stem, fmt)
        except ValueError:
            pass
    raise ValueError(f"Cannot read a snapshot date from {name!r} (expected YYYY-MM[-DD].tsv.gz)")


def load_snapshots(
    spark,
    snapshots,
    best_rank_only: bool = True,
    sample_fraction: Optional[float] = None,
    valid_at_snapshot: bool = False,
):
    """Union several extracted snapshots, ready for windowing and mining.

    Args:
        snapshots: ``snapshot_id -> (tsv path, snapshot datetime)``.
        best_rank_only: Keep only best-rank statements (Wikidata's "truthy"
            view). Deprecated statements are dropped either way.
        sample_fraction: Keep this fraction of *instances* (P31 subjects),
            chosen by a hash of the entity IRI so the same entities are kept in
            every snapshot. Subclass (P279) edges are always kept, so the class
            hierarchy stays complete. ``None`` keeps everything.
        valid_at_snapshot: Drop statements whose valid-time qualifiers say they
            did not hold on the snapshot date (``valid_to`` before it, or
            ``valid_from`` after it). Statements without qualifiers are kept.
    """
    from functools import reduce

    from pyspark.sql import functions as F

    if not snapshots:
        raise ValueError("No snapshots given.")
    frames = []
    for snap, (path, ts) in snapshots.items():
        df = read_statements(spark, path, snap, ts)
        if best_rank_only:
            df = df.where(F.col("best_rank"))
        else:
            df = df.where(F.col("rank") != "deprecated")
        if valid_at_snapshot:
            day = F.to_date(F.col("snapshot_ts"))
            df = df.where(
                (F.col("valid_to").isNull() | (F.col("valid_to") >= day))
                & (F.col("valid_from").isNull() | (F.col("valid_from") <= day))
            )
        if sample_fraction is not None:
            if not 0 < sample_fraction <= 1:
                raise ValueError("sample_fraction must be in (0, 1].")
            bucket = F.pmod(F.xxhash64("subject"), F.lit(1_000_000))
            df = df.where(
                (F.col("predicate") != DIRECT_NS + "P31")
                | (bucket < F.lit(int(sample_fraction * 1_000_000)))
            )
        frames.append(df)
    return reduce(lambda a, b: a.unionByName(b), frames)
