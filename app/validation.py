import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.errors import PayloadTooLarge, ValidationError

UTC = timezone.utc
EARLIEST = datetime(2000, 1, 1, tzinfo=UTC)
FIELD_LIMITS = {
    "event_id": 128,
    "tenant_id": 64,
    "source": 64,
    "event_type": 64,
}


@dataclass(frozen=True)
class Event:
    event_id: str
    tenant_id: str
    source: str
    event_type: str
    timestamp: str
    payload: dict

    def payload_json(self):
        return json.dumps(self.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def format_timestamp(value):
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_timestamp(value, *, enforce_bounds=True, now=None, skew_minutes=5):
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("timestamp is required", details=[{"field": "timestamp", "message": "timestamp is required"}])
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValidationError(
            "timestamp is not a valid datetime",
            details=[{"field": "timestamp", "message": "use an ISO-8601 datetime"}],
        ) from exc
    if parsed.tzinfo is None:
        raise ValidationError(
            "timestamp must include a timezone",
            details=[{"field": "timestamp", "message": "naive timestamps are rejected; send UTC, for example 2024-01-15T10:15:30Z"}],
        )
    parsed = parsed.astimezone(UTC)
    if enforce_bounds:
        current = now or datetime.now(UTC)
        if parsed < EARLIEST:
            raise ValidationError(
                "timestamp is too far in the past",
                details=[{"field": "timestamp", "message": "timestamps before 2000-01-01 are rejected"}],
            )
        if parsed > current + timedelta(minutes=skew_minutes):
            raise ValidationError(
                "timestamp is too far in the future",
                details=[{"field": "timestamp", "message": f"timestamps more than {skew_minutes} minutes ahead are rejected"}],
            )
    return parsed


def _require_text(data, field):
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        return None, {"field": field, "message": f"{field} is required"}
    value = value.strip()
    limit = FIELD_LIMITS[field]
    if len(value) > limit:
        return None, {"field": field, "message": f"{field} must be at most {limit} characters"}
    return value, None


def parse_event(data, *, max_payload_bytes, skew_minutes):
    if not isinstance(data, dict):
        raise ValidationError("event must be a JSON object", details=[{"field": "event", "message": "event must be a JSON object"}])
    details = []
    values = {}
    for field in ("event_id", "tenant_id", "source", "event_type"):
        value, error = _require_text(data, field)
        if error:
            details.append(error)
        else:
            values[field] = value
    if "timestamp" not in data:
        details.append({"field": "timestamp", "message": "timestamp is required"})
        parsed_time = None
    else:
        try:
            parsed_time = parse_timestamp(data.get("timestamp"), skew_minutes=skew_minutes)
        except ValidationError as exc:
            details.extend(exc.details)
            parsed_time = None
    payload = data.get("payload")
    if not isinstance(payload, dict):
        details.append({"field": "payload", "message": "payload must be a JSON object"})
    else:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(encoded) > max_payload_bytes:
            raise PayloadTooLarge(
                "event payload exceeds the size limit",
                details=[{"field": "payload", "message": f"payload must be at most {max_payload_bytes} bytes"}],
            )
    if details:
        raise ValidationError("event is invalid", details=details)
    return Event(
        event_id=values["event_id"],
        tenant_id=values["tenant_id"],
        source=values["source"],
        event_type=values["event_type"],
        timestamp=format_timestamp(parsed_time),
        payload=payload,
    )


def parse_bulk(body, *, max_events, max_payload_bytes, skew_minutes):
    if isinstance(body, list):
        items = body
    elif isinstance(body, dict) and "events" in body:
        items = body["events"]
    else:
        raise ValidationError(
            "bulk body must be an array or an object with an events array",
            details=[{"field": "events", "message": "expected a list of events"}],
        )
    if not isinstance(items, list):
        raise ValidationError("events must be an array", details=[{"field": "events", "message": "events must be an array"}])
    if len(items) == 0:
        raise ValidationError("events must not be empty", details=[{"field": "events", "message": "provide at least one event"}])
    if len(items) > max_events:
        raise PayloadTooLarge(
            f"bulk requests accept at most {max_events} events",
            details=[{"field": "events", "message": f"received {len(items)} events"}],
        )

    details = []
    first_by_id = {}
    order = []
    extra_duplicates = 0
    conflicts = []
    for index, item in enumerate(items):
        try:
            event = parse_event(item, max_payload_bytes=max_payload_bytes, skew_minutes=skew_minutes)
        except PayloadTooLarge as exc:
            for detail in exc.details:
                detail = dict(detail)
                detail["index"] = index
                details.append(detail)
            continue
        except ValidationError as exc:
            for detail in exc.details:
                detail = dict(detail)
                detail["index"] = index
                details.append(detail)
            continue
        previous = first_by_id.get(event.event_id)
        if previous is None:
            first_by_id[event.event_id] = event
            order.append(event.event_id)
        elif previous == event:
            extra_duplicates += 1
        else:
            conflicts.append({"index": index, "event_id": event.event_id, "message": "same event_id appears with different content in this batch"})
    if details:
        shown = details[:50]
        message = "one or more events are invalid"
        if len(details) > 50:
            message += f" ({len(details)} problems, showing the first 50)"
        raise ValidationError(message, details=shown)
    if conflicts:
        raise ValidationError(
            "bulk batch contains conflicting duplicates",
            details=conflicts[:50],
            code="conflict",
            status=409,
        )
    return [first_by_id[event_id] for event_id in order], extra_duplicates


def floor_bucket(value, size):
    value = value.astimezone(UTC)
    if size == "minute":
        return value.replace(second=0, microsecond=0)
    if size == "hour":
        return value.replace(minute=0, second=0, microsecond=0)
    raise ValidationError("bucket must be minute or hour", details=[{"field": "bucket", "message": "use minute or hour"}])


def add_bucket(value, size):
    if size == "minute":
        return value + timedelta(minutes=1)
    return value + timedelta(hours=1)
