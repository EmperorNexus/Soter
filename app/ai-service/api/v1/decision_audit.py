"""Decision audit query endpoints (issue #990, export #1206).

Satisfies the "records are queryable by trace id, claim id, and campaign
reference" acceptance criterion: an operator reconstructing why a claim was
rejected weeks later hits `GET /v1/ai/decision-audit?claim_id=...` (or by
`trace_id` / `campaign_ref`) and gets the inputs, provider, model, prompt
version, outcome, and reasons behind every decision made for that claim.

Issue #1206 adds an export mode (`format=csv` or `format=ndjson`) that
streams a downloadable filtered set for compliance review without paging
individual record lookups. Records are already redacted at write time (see
`services/decision_audit.py`), so these endpoints expose no raw PII.

Issue #1302 adds a dashboard-friendly throughput aggregate endpoint that
summarises claims processed per hour, the pass/review/reject distribution,
and the average time-to-decision over a requested time window. Results are
cached per window so dashboards do not rescan the full audit history on
every call.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from typing import Any, Dict, Iterator, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from schemas.common import ResultEnvelope
from services.decision_audit import DecisionAuditRecord, get_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["decision-audit"])

#: Upper bound on how many records one JSON query can return, so a broad query
#: cannot serialise an entire retention window into a single response.
MAX_LIMIT = 500

#: Page size cap for export mode. Exports stream, so larger pages are safe; the
#: client still paginates with `offset` / `limit` when the full set is huge.
MAX_EXPORT_LIMIT = 5000

EXPORT_FORMATS = frozenset({"csv", "ndjson"})

#: Flat CSV columns. Nested `inputs` / `reasons` / `metadata` are
#: serialised as JSON strings so the download stays one row per record.
CSV_COLUMNS = (
    "record_id",
    "created_at",
    "decision_type",
    "outcome",
    "trace_id",
    "claim_id",
    "campaign_ref",
    "package_id",
    "org_id",
    "provider",
    "model",
    "prompt_version",
    "prompt_variant",
    "confidence",
    "reasons",
    "inputs",
    "metadata",
    "schema_version",
)

#: How long a throughput aggregate is reused before being recomputed.
#: Dashboards poll frequently; a short TTL bounds audit-store scan cost while
#: keeping the view near-real-time.
THROUGHPUT_CACHE_TTL_SECONDS = 30.0

#: Maximum number of cache entries retained. Keyed by the requested window.
THROUGHPUT_CACHE_MAX_ENTRIES = 256

#: Outcomes that count as a completed decision for time-to-decision.
DECISION_OUTCOMES = frozenset({"pass", "approve", "review", "reject"})

#: Page size cap for export mode. Exports stream, so larger pages are safe; the
#: client still paginates with ``offset`` / ``limit`` when the full set is huge.
_MAX_EXPORT_LIMIT = 5000

_EXPORT_FORMATS = frozenset({"csv", "ndjson"})

#: Flat CSV columns. Nested ``inputs`` / ``reasons`` / ``metadata`` are
#: serialised as JSON strings so the download stays one row per record.
_CSV_COLUMNS = (
    "record_id",
    "created_at",
    "decision_type",
    "outcome",
    "trace_id",
    "claim_id",
    "campaign_ref",
    "package_id",
    "org_id",
    "provider",
    "model",
    "prompt_version",
    "prompt_variant",
    "confidence",
    "reasons",
    "inputs",
    "metadata",
    "schema_version",
)


def _resolve_store(http_request: Request):
    """Resolve the audit store from app state, falling back to the module store."""
    store = getattr(http_request.app.state, "decision_audit_store", None)
    if store is None:
        store = get_store()
    if store is None:
        raise HTTPException(
            status_code=503, detail="Decision audit store is not configured"
        )
    return store


def _require_query_identifier(
    trace_id: Optional[str],
    claim_id: Optional[str],
    campaign_ref: Optional[str],
) -> None:
    if not any([trace_id, claim_id, campaign_ref]):
        raise HTTPException(
            status_code=400,
            detail="At least one of trace_id, claim_id, or campaign_ref is required",
        )


def _record_to_csv_row(record: DecisionAuditRecord) -> Dict[str, Any]:
    data = record.to_dict()
    row: Dict[str, Any] = {}
    for col in CSV_COLUMNS:
        value = data.get(col)
        if col in ("reasons", "inputs", "metadata"):
            row[col] = json.dumps(value, sort_keys=True, default=str)
        else:
            row[col] = "" if value is None else value
    return row


def _stream_ndjson(records: Iterator[DecisionAuditRecord]) -> Iterator[str]:
    """Yield one NDJSON line at a time so large exports are not buffered."""
    for record in records:
        # Records were redacted at write time; export must not undo that.
        yield json.dumps(record.to_dict(), sort_keys=True, default=str) + "\n"


def _stream_csv(records: Iterator[DecisionAuditRecord]) -> Iterator[str]:
    """Yield CSV header then one row per record without loading the full file."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=list(CSV_COLUMNS), extrasaction="ignore"
    )
    writer.writeheader()
    yield buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)
    for record in records:
        writer.writerow(_record_to_csv_row(record))
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)


class _ThroughputCache:
    """Bounded, TTL-backed cache for throughput aggregates."""

    def __init__(self, ttl_seconds: float, max_entries: int) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._entries: Dict[tuple, tuple] = {}

    def get(self, key: tuple) -> Optional[Dict[str, Any]]:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at <= time.monotonic():
            self._entries.pop(key, None)
            return None
        return value

    def set(self, key: tuple, value: Dict[str, Any]) -> None:
        if len(self._entries) >= self._max_entries:
            # Evict the oldest expiring entry first so the cache stays bounded.
            oldest = min(self._entries.items(), key=lambda item: item[1][0])
            self._entries.pop(oldest[0], None)
        self._entries[key] = (time.monotonic() + self._ttl, value)

    def clear(self) -> None:
        self._entries.clear()


_THROUGHPUT_CACHE = _ThroughputCache(
    ttl_seconds=THROUGHPUT_CACHE_TTL_SECONDS,
    max_entries=THROUGHPUT_CACHE_MAX_ENTRIES,
)


def _compute_throughput(
    records: List[Dict[str, Any]],
    window_start: float,
    window_end: float,
) -> Dict[str, Any]:
    """Compute throughput and outcome distribution from audit records."""
    distribution: Dict[str, int] = {}
    total_labelled = 0
    total_latency = 0.0
    latency_count = 0
    for record in records:
        outcome = record.get("outcome")
        if outcome is None:
            continue
        outcome_key = str(outcome).lower()
        distribution[outcome_key] = distribution.get(outcome_key, 0) + 1
        total_labelled += 1
        latency = record.get("latency_ms")
        if latency is not None:
            try:
                total_latency += float(latency)
                latency_count += 1
            except (TypeError, ValueError):
                pass

    duration_seconds = max(window_end - window_start, 0.0)
    duration_hours = duration_seconds / 3600.0
    claims_per_hour = (
        round(total_labelled / duration_hours, 4) if duration_hours > 0 else 0.0
    )
    average_latency_ms = (
        round(total_latency / latency_count, 2) if latency_count > 0 else None
    )

    return {
        "window": {
            "start": window_start,
            "end": window_end,
            "duration_seconds": duration_seconds,
        },
        "total_claims": total_labelled,
        "claims_per_hour": claims_per_hour,
        "outcome_distribution": dict(sorted(distribution.items())),
        "average_time_to_decision_ms": average_latency_ms,
    }


@router.get(
    "/ai/decision-audit/throughput",
    response_model=ResultEnvelope[Dict[str, Any]],
)
async def get_throughput_aggregate(
    http_request: Request,
    created_after: Optional[float] = Query(
        None,
        description="Window start as a Unix timestamp. Defaults to one hour ago.",
    ),
    created_before: Optional[float] = Query(
        None,
        description="Window end as a Unix timestamp. Defaults to now.",
    ),
    decision_type: Optional[str] = Query(
        None,
        description="Optional decision type filter, e.g. humanitarian_verification.",
    ),
    refresh: bool = Query(
        False,
        description="Bypass the cache and recompute the aggregate.",
    ),
) -> ResultEnvelope[Dict[str, Any]]:
    """Return throughput and outcome aggregates for a time window.

    The response is shaped for direct dashboard consumption: claims per
    hour, the pass/review/reject distribution, and the average time to
    decision. Results are cached per window for a short TTL so repeated
    dashboard polls do not rescan the audit store. Pass `refresh=true` to
    force a recompute.
    """
    now = time.time()
    window_end = created_before if created_before is not None else now
    window_start = created_after if created_after is not None else window_end - 3600.0
    if window_start > window_end:
        raise HTTPException(
            status_code=400,
            detail="created_after must be <= created_before",
        )

    cache_key = (window_start, window_end, decision_type)
    if not refresh:
        cached = _THROUGHPUT_CACHE.get(cache_key)
        if cached is not None:
            return ResultEnvelope[Dict[str, Any]](
                result=cached,
                confidence=None,
                reasons=None,
                anchor_metadata=None,
                trace_id=getattr(http_request.state, "correlation_id", "") or None,
            )

    store = _resolve_store(http_request)
    records = store.query(
        trace_id=None,
        claim_id=None,
        campaign_ref=None,
        decision_type=decision_type,
        created_after=window_start,
        created_before=window_end,
        offset=0,
        limit=None,
    )
    record_dicts = [r.to_dict() for r in records]
    aggregate = _compute_throughput(record_dicts, window_start, window_end)
    aggregate["cached"] = False
    aggregate["computed_at"] = now
    _THROUGHPUT_CACHE.set(cache_key, aggregate)

    return ResultEnvelope[Dict[str, Any]](
        result=aggregate,
        confidence=None,
        reasons=None,
        anchor_metadata=None,
        trace_id=getattr(http_request.state, "correlation_id", "") or None,
    )


@router.get(
    "/ai/decision-audit",
    response_model=None,
)
async def query_decision_audit(
    http_request: Request,
    trace_id: Optional[str] = Query(
        None, description="Correlation/trace ID echoed on the original response."
    ),
    claim_id: Optional[str] = Query(
        None, description="Claim ID from anchor_metadata or fraud claim metadata."
    ),
    campaign_ref: Optional[str] = Query(
        None, description="Campaign reference from anchor_metadata."
    ),
    decision_type: Optional[str] = Query(
        None,
        description="Filter by decision type, e.g. humanitarian_verification"
        "or fraud_detection.",
    ),
    created_after: Optional[float] = Query(
        None,
        description="Include only records with created_at >= this Unix timestamp.",
    ),
    created_before: Optional[float] = Query(
        None,
        description="Include only records with created_at <= this Unix timestamp.",
    ),
    format: Optional[str] = Query(
        None,
        description="Response format: omit/json for ResultEnvelope JSON; "
        "csv or ndjson for a downloadable streamed export (issue #1206).",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of matching records to skip (pagination for export).",
    ),
    limit: int = Query(100, ge=1, le=MAX_EXPORT_LIMIT),
):
    """Return or export decision audit records matching the supplied filters.

    At least one of `trace_id`, `claim_id`, or `campaign_ref` is
    required. Supplying several narrows the result (logical AND). Records
    come back newest first.

    Set `format=csv` or `format=ndjson` for a streamed downloadable export
    that uses the same filters and redaction as the JSON query. Paginate large
    exports with `offset` and `limit` (export limit cap is higher than the
    JSON query cap).
    """
    _require_query_identifier(trace_id, claim_id, campaign_ref)

    export_format = (format or "json").strip().lower()
    if export_format not in ("json", "csv", "ndjson"):
        raise HTTPException(
            status_code=400,
            detail="format must be one of: json, csv, ndjson",
        )

    # JSON responses keep the tighter cap so a broad query cannot dump the
    # whole retention window into one envelope.
    if export_format == "json" and limit > MAX_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"limit must be <= {MAX_LIMIT} for JSON responses",
        )

    store = _resolve_store(http_request)
    records = store.query(
        trace_id=trace_id,
        claim_id=claim_id,
        campaign_ref=campaign_ref,
        decision_type=decision_type,
        created_after=created_after,
        created_before=created_before,
        offset=offset,
        limit=limit,
    )

    if export_format in EXPORT_FORMATS:
        # Stream so large exports are not loaded fully into a response buffer.
        filename = f"decision-audit.{export_format}"
        if export_format == "ndjson":
            media_type = "application/x-ndjson"
            body = _stream_ndjson(iter(records))
        else:
            media_type = "text/csv; charset=utf-8"
            body = _stream_csv(iter(records))
        return StreamingResponse(
            body,
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Count": str(len(records)),
                "X-Export-Offset": str(offset),
                "X-Export-Limit": str(limit),
            },
        )

    return ResultEnvelope[List[Dict[str, Any]]](
        result=[r.to_dict() for r in records],
        confidence=None,
        reasons=None,
        anchor_metadata=None,
        trace_id=getattr(http_request.state, "correlation_id", "") or None,
    )


@router.get(
    "/ai/decision-audit/{record_id}",
    response_model=ResultEnvelope[Dict[str, Any]],
)
async def get_decision_audit_record(
    http_request: Request,
    record_id: str,
) -> ResultEnvelope[Dict[str, Any]]:
    """Return a single decision audit record by its `record_id`."""
    store = _resolve_store(http_request)
    record = store.get(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Audit record not found")

    return ResultEnvelope[Dict[str, Any]](
        result=record.to_dict(),
        confidence=None,
        reasons=None,
        anchor_metadata=None,
        trace_id=getattr(http_request.state, "correlation_id", "") or None,
    )