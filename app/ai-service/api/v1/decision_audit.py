"""Decision audit query endpoints (issue #990, export #1206).

Satisfies the "records are queryable by trace id, claim id, and campaign
reference" acceptance criterion: an operator reconstructing why a claim was
rejected weeks later hits ``GET /v1/ai/decision-audit?claim_id=...`` (or by
``trace_id`` / ``campaign_ref``) and gets the inputs, provider, model, prompt
version, outcome, and reasons behind every decision made for that claim.

Issue #1206 adds an export mode (``format=csv`` or ``format=ndjson``) that
streams a downloadable filtered set for compliance review without paging
individual record lookups. Records are already redacted at write time (see
``services/decision_audit.py``), so these endpoints expose no raw PII.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from schemas.common import ResultEnvelope
from services.decision_audit import DecisionAuditRecord, get_store

logger = logging.getLogger(__name__)

router = APIRouter(tags=["decision-audit"])

#: Upper bound on how many records one JSON query can return, so a broad query
#: cannot serialise an entire retention window into a single response.
_MAX_LIMIT = 500

#: Page size cap for export mode. Exports stream, so larger pages are safe; the
#: client still paginates with ``offset`` / ``limit`` when the full set is huge.
_MAX_EXPORT_LIMIT = 5000

_EXPORT_FORMATS = frozenset({"csv", "ndjson"})

#: Default and maximum lookback windows (seconds) for the throughput
#: aggregate endpoint. Bounding the window keeps the scan from touching the
#: full retention history on every dashboard refresh.
_DEFAULT_THROUGHPUT_WINDOW = 3600.0
_MAX_THROUGHPUT_WINDOW = 7 * 24 * 3600.0

#: How long a computed throughput aggregate stays cached, keyed by the
#: resolved (start, end) window. Dashboards poll frequently; this avoids
#: re-scanning the audit store on every call.
_THROUGHPUT_CACHE_TTL = 30.0

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


def _throughput_cache(http_request: Request) -> Dict[Tuple[float, float], Tuple[float, Dict[str, Any]]]:
    """Return the per-app throughput cache, creating it on first use."""
    cache = getattr(http_request.app.state, "decision_audit_throughput_cache", None)
    if cache is None:
        cache = {}
        http_request.app.state.decision_audit_throughput_cache = cache
    return cache


def _compute_throughput(
    records: List[DecisionAuditRecord],
    window_start: float,
    window_end: float,
) -> Dict[str, Any]:
    """Aggregate a bounded set of records into dashboard-friendly metrics."""
    total = len(records)
    outcomes: Dict[str, int] = {}
    decision_times: List[float] = []
    for record in records:
        outcome = (record.outcome or "unknown").strip().lower() or "unknown"
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
        decision_times.append(record.created_at)

    window_seconds = max(window_end - window_start, 0.0)
    hours = window_seconds / 3600.0
    claims_per_hour = (total / hours) if hours > 0 else 0.0

    # Time-to-decision is approximated from the audit record timestamps: the
    # span between the oldest and newest decision in the window, averaged per
    # record. This stays bounded and needs no extra store lookups.
    if total > 1:
        span = max(decision_times) - min(decision_times)
        avg_time_to_decision = span / (total - 1)
    else:
        avg_time_to_decision = 0.0

    return {
        "window_start": window_start,
        "window_end": window_end,
        "window_seconds": window_seconds,
        "total_decisions": total,
        "claims_per_hour": round(claims_per_hour, 4),
        "outcome_distribution": dict(sorted(outcomes.items())),
        "average_time_to_decision_seconds": round(avg_time_to_decision, 4),
    }


def _record_to_csv_row(record: DecisionAuditRecord) -> Dict[str, Any]:
    data = record.to_dict()
    row: Dict[str, Any] = {}
    for col in _CSV_COLUMNS:
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
        buffer, fieldnames=list(_CSV_COLUMNS), extrasaction="ignore"
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
        description="Filter by decision type, e.g. humanitarian_verification "
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
    limit: int = Query(100, ge=1, le=_MAX_EXPORT_LIMIT),
):
    """Return or export decision audit records matching the supplied filters.

    At least one of ``trace_id``, ``claim_id``, or ``campaign_ref`` is
    required. Supplying several narrows the result (logical AND). Records
    come back newest first.

    Set ``format=csv`` or ``format=ndjson`` for a streamed downloadable export
    that uses the same filters and redaction as the JSON query. Paginate large
    exports with ``offset`` and ``limit`` (export limit cap is higher than the
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
    if export_format == "json" and limit > _MAX_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"limit must be <= {_MAX_LIMIT} for JSON responses",
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

    if export_format in _EXPORT_FORMATS:
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
    "/ai/decision-audit/throughput",
    response_model=ResultEnvelope[Dict[str, Any]],
)
async def get_decision_audit_throughput(
    http_request: Request,
    window_seconds: float = Query(
        _DEFAULT_THROUGHPUT_WINDOW,
        gt=0,
        le=_MAX_THROUGHPUT_WINDOW,
        description="Lookback window in seconds (bounded to avoid full-history scans).",
    ),
    decision_type: Optional[str] = Query(
        None,
        description="Optional decision type filter, e.g. humanitarian_verification.",
    ),
) -> ResultEnvelope[Dict[str, Any]]:
    """Return throughput and outcome-distribution aggregates for a time window.

    Summarises claims processed per hour, the pass/review/reject distribution,
    and the average time-to-decision so a dashboard can render directly without
    aggregating raw audit records. Results are cached briefly per window so
    repeated dashboard polls do not rescan the audit store.
    """
    window_end = time.time()
    window_start = window_end - window_seconds

    cache = _throughput_cache(http_request)
    cache_key = (round(window_start, 3), round(window_end, 3))
    cached = cache.get(cache_key)
    if cached is not None and (time.time() - cached[0]) < _THROUGHPUT_CACHE_TTL:
        aggregate = cached[1]
    else:
        store = _resolve_store(http_request)
        records = store.query(
            decision_type=decision_type,
            created_after=window_start,
            created_before=window_end,
            offset=0,
            limit=_MAX_EXPORT_LIMIT,
        )
        aggregate = _compute_throughput(records, window_start, window_end)
        # Drop stale entries so the cache stays bounded.
        now = time.time()
        for key in [k for k, v in cache.items() if (now - v[0]) >= _THROUGHPUT_CACHE_TTL]:
            cache.pop(key, None)
        cache[cache_key] = (now, aggregate)

    return ResultEnvelope[Dict[str, Any]](
        result=aggregate,
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
    """Return a single decision audit record by its ``record_id``."""
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
