import asyncio
import base64

from flask import current_app, g, jsonify, render_template, request

from app.aggregate import aggregate_all

from app.errors import ConflictError, ValidationError
from app.ingest import insert_bulk, insert_event
from app.queries import list_events, list_metrics, parse_query_time
from app.validation import parse_bulk, parse_event


def register_routes(app):
    @app.get("/")
    def home():
        return render_template("index.html")

    @app.post("/worker/once")
    def worker_once():
        processed = asyncio.run(
            aggregate_all(current_app.config["DATABASE"], current_app.config["AGGREGATE_BATCH_SIZE"])
        )
        return jsonify({"processed": processed})

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.get("/ready")
    def ready():
        from app.db import connect

        try:
            connection = connect(current_app.config["DATABASE"])
            try:
                connection.execute("SELECT 1").fetchone()
            finally:
                connection.close()
        except Exception:
            return jsonify({"status": "unavailable", "database": "down"}), 503
        return jsonify({"status": "ready", "database": "up"})

    @app.post("/events")
    def post_event():
        limited = _enforce_rate_limit("single")
        if limited is not None:
            return limited
        event = parse_event(
            _json_body(),
            max_payload_bytes=current_app.config["MAX_EVENT_PAYLOAD_BYTES"],
            skew_minutes=current_app.config["FUTURE_SKEW_MINUTES"],
        )
        outcome = insert_event(current_app.config["DATABASE"], event)
        if outcome == "conflict":
            raise ConflictError(
                "event_id already exists with different content",
                details=[{"event_id": event.event_id, "message": "event_id already exists with different content"}],
            )
        status = 201 if outcome == "created" else 200
        return jsonify({"status": outcome, "event_id": event.event_id}), status

    @app.post("/events/bulk")
    def post_bulk():
        limited = _enforce_rate_limit("bulk")
        if limited is not None:
            return limited
        body = _json_body()
        events, extra_duplicates = parse_bulk(
            body,
            max_events=current_app.config["MAX_BULK_EVENTS"],
            max_payload_bytes=current_app.config["MAX_EVENT_PAYLOAD_BYTES"],
            skew_minutes=current_app.config["FUTURE_SKEW_MINUTES"],
        )
        received = len(body if isinstance(body, list) else body["events"])
        result = insert_bulk(current_app.config["DATABASE"], events)
        inserted = result.inserted
        duplicates = result.duplicates + extra_duplicates
        status = 201 if inserted else 200
        return jsonify(
            {
                "status": "ok",
                "received": received,
                "inserted": inserted,
                "duplicates": duplicates,
            }
        ), status

    @app.get("/events")
    def get_events():
        tenant_id = _required_text("tenant_id")
        start = parse_query_time(request.args.get("from"), "from")
        end = parse_query_time(request.args.get("to"), "to")
        if start is not None and end is not None and start >= end:
            raise ValidationError("from must be earlier than to", details=[{"field": "from", "message": "from must be earlier than to"}])
        limit = _parse_limit()
        cursor = _decode_cursor(request.args.get("cursor"))
        items, next_cursor = list_events(
            current_app.config["DATABASE"],
            tenant_id=tenant_id,
            source=_optional_text("source"),
            event_type=_optional_text("event_type"),
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
        )
        return jsonify(
            {
                "items": items,
                "limit": limit,
                "next_cursor": _encode_cursor(next_cursor) if next_cursor else None,
            }
        )

    @app.get("/metrics")
    def get_metrics():
        tenant_id = _required_text("tenant_id")
        bucket = (request.args.get("bucket") or "hour").strip().lower()
        if bucket not in {"minute", "hour"}:
            raise ValidationError("bucket must be minute or hour", details=[{"field": "bucket", "message": "use minute or hour"}])
        report = list_metrics(
            current_app.config["DATABASE"],
            tenant_id=tenant_id,
            bucket_size=bucket,
            source=_optional_text("source"),
            event_type=_optional_text("event_type"),
            group_by=_parse_group_by(request.args.get("group_by")),
            start=parse_query_time(request.args.get("from"), "from"),
            end=parse_query_time(request.args.get("to"), "to"),
            default_hours=current_app.config["METRICS_DEFAULT_HOURS"],
            max_minute_days=current_app.config["MAX_MINUTE_RANGE_DAYS"],
            max_hour_days=current_app.config["MAX_HOUR_RANGE_DAYS"],
        )
        return jsonify(report)


def _json_body():
    if request.content_length == 0 or not request.data:
        raise ValidationError("request body is required", details=[{"field": "body", "message": "send a JSON object"}])
    body = request.get_json(silent=True)
    if body is None:
        raise ValidationError("request body must be valid JSON", details=[{"field": "body", "message": "request body must be valid JSON"}])
    return body


def _required_text(name):
    value = (request.args.get(name) or "").strip()
    if not value:
        raise ValidationError(f"{name} is required", details=[{"field": name, "message": f"{name} is required"}])
    return value


def _optional_text(name):
    value = (request.args.get(name) or "").strip()
    return value or None


def _parse_limit():
    raw = request.args.get("limit")
    default = current_app.config["DEFAULT_PAGE_SIZE"]
    maximum = current_app.config["MAX_PAGE_SIZE"]
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValidationError("limit must be an integer", details=[{"field": "limit", "message": "limit must be an integer"}]) from exc
    if value < 1 or value > maximum:
        raise ValidationError(
            f"limit must be between 1 and {maximum}",
            details=[{"field": "limit", "message": f"limit must be between 1 and {maximum}"}],
        )
    return value


def _parse_group_by(value):
    if not value:
        return set()
    parts = [part.strip() for part in value.split(",") if part.strip()]
    allowed = {"source", "event_type"}
    if any(part not in allowed for part in parts):
        raise ValidationError(
            "group_by must be source, event_type, or both",
            details=[{"field": "group_by", "message": "allowed values are source and event_type"}],
        )
    return set(parts)


def _encode_cursor(cursor):
    timestamp, event_id = cursor
    raw = f"{timestamp}\n{event_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(cursor):
    if not cursor:
        return None
    try:
        text = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
        timestamp, event_id = text.split("\n", 1)
    except Exception as exc:
        raise ValidationError("cursor is invalid", details=[{"field": "cursor", "message": "cursor is invalid"}]) from exc
    if len(timestamp) != 27 or not event_id:
        raise ValidationError("cursor is invalid", details=[{"field": "cursor", "message": "cursor is invalid"}])
    return timestamp, event_id


def _enforce_rate_limit(scope):
    if not current_app.config["RATE_LIMIT_ENABLED"]:
        return None
    limiter = current_app.extensions["limiters"][scope]
    allowed, remaining = limiter.allow(request.remote_addr or "unknown")
    g.rate_limit = limiter.limit
    g.rate_remaining = remaining
    if allowed:
        return None
    g.rate_remaining = 0
    response = jsonify({"error": "rate_limited", "message": "Too many requests. Try again shortly."})
    response.status_code = 429
    response.headers["Retry-After"] = str(current_app.config["RATE_WINDOW_SECONDS"])
    return response
