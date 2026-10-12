"""Pure GTFS-RT protobuf decoding used by Glue and unit tests."""

import hashlib

from google.transit import gtfs_realtime_pb2


def _optional(message, field_name):
    try:
        return getattr(message, field_name) if message.HasField(field_name) else None
    except (ValueError, AttributeError):
        return None


def _enum_name(message, field_name):
    value = _optional(message, field_name)
    if value is None:
        return None
    field = message.DESCRIPTOR.fields_by_name[field_name]
    enum_value = field.enum_type.values_by_number.get(int(value))
    return enum_value.name if enum_value else str(value)


def feed_metadata(payload):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return {
        "feed_timestamp": _optional(feed.header, "timestamp"),
        "feed_version": _optional(feed.header, "feed_version"),
        "entity_count": len(feed.entity),
    }


def decode_snapshot(payload, raw_object_key, ingested_at_utc, expected_sha256=None):
    """Decode one raw snapshot into event rows and a per-file quality record.

    A StopTimeUpdate with neither arrival nor departure is retained as one row
    with event_type ``none``.  It is source evidence, not an analytical delay
    event, and must be excluded from arrival/departure facts later.
    """
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    quality = {
        "raw_object_key": raw_object_key,
        "ingested_at_utc": ingested_at_utc,
        "payload_sha256": actual_sha256,
        "expected_payload_sha256": expected_sha256,
        "payload_hash_valid": (
            expected_sha256 is None or actual_sha256 == expected_sha256
        ),
        "parse_status": "started",
        "error_class": None,
        "error_detail": None,
        "feed_timestamp": None,
        "feed_version": None,
        "entity_count": 0,
        "trip_update_entity_count": 0,
        "stop_time_update_count": 0,
        "event_row_count": 0,
        "missing_delay_count": 0,
        "zero_delay_count": 0,
    }

    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(payload)
        quality.update(
            {
                "feed_timestamp": _optional(feed.header, "timestamp"),
                "feed_version": _optional(feed.header, "feed_version"),
                "entity_count": len(feed.entity),
            }
        )

        rows = []
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            quality["trip_update_entity_count"] += 1
            update = entity.trip_update
            trip = update.trip
            vehicle = update.vehicle

            common = {
                "source": "swiss_gtfs_rt",
                "raw_object_key": raw_object_key,
                "payload_sha256": actual_sha256,
                "payload_hash_valid": quality["payload_hash_valid"],
                "ingested_at_utc": ingested_at_utc,
                "feed_timestamp": quality["feed_timestamp"],
                "feed_version": quality["feed_version"],
                "entity_id": entity.id,
                "entity_is_deleted": bool(_optional(entity, "is_deleted") or False),
                "trip_id": _optional(trip, "trip_id"),
                "route_id": _optional(trip, "route_id"),
                "direction_id": _optional(trip, "direction_id"),
                "service_date": _optional(trip, "start_date"),
                "start_time": _optional(trip, "start_time"),
                "trip_schedule_relationship": _enum_name(
                    trip, "schedule_relationship"
                ),
                "trip_update_timestamp": _optional(update, "timestamp"),
                "trip_level_delay_seconds": _optional(update, "delay"),
                "vehicle_id": _optional(vehicle, "id"),
                "vehicle_label": _optional(vehicle, "label"),
            }

            for stop_update in update.stop_time_update:
                quality["stop_time_update_count"] += 1
                event_messages = []
                if stop_update.HasField("arrival"):
                    event_messages.append(("arrival", stop_update.arrival))
                if stop_update.HasField("departure"):
                    event_messages.append(("departure", stop_update.departure))
                if not event_messages:
                    event_messages.append(("none", None))

                for event_type, event_message in event_messages:
                    delay = (
                        _optional(event_message, "delay")
                        if event_message is not None
                        else None
                    )
                    if delay is None:
                        quality["missing_delay_count"] += 1
                    elif delay == 0:
                        quality["zero_delay_count"] += 1

                    rows.append(
                        {
                            **common,
                            "stop_id": _optional(stop_update, "stop_id"),
                            "stop_sequence": _optional(
                                stop_update, "stop_sequence"
                            ),
                            "stop_schedule_relationship": _enum_name(
                                stop_update, "schedule_relationship"
                            ),
                            "event_type": event_type,
                            "published_time_epoch": (
                                _optional(event_message, "time")
                                if event_message is not None
                                else None
                            ),
                            "delay_seconds": delay,
                            "uncertainty_seconds": (
                                _optional(event_message, "uncertainty")
                                if event_message is not None
                                else None
                            ),
                            "is_delay_missing": delay is None,
                        }
                    )

        quality["event_row_count"] = len(rows)
        quality["parse_status"] = "success"
        return rows, quality

    except Exception as error:
        quality["parse_status"] = "failed"
        quality["error_class"] = type(error).__name__
        quality["error_detail"] = str(error)[:500]
        return [], quality
