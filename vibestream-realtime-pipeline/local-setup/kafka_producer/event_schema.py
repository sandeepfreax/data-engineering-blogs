"""
VibeStream Event Schema Definition

Defines the canonical event schema for all user engagement events.

Design Decisions:
- JSON over Avro for local dev simplicity; Avro + Schema Registry recommended for production.
- event_type is an Enum to prevent schema drift at the producer level.
- content_type differentiates video/image/text posts (used for ML feature partitioning).
- device_type feeds into session analytics downstream.
"""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional
import json
import uuid
from datetime import datetime, timezone


class EventType(str, Enum):
    """
    All engagement event types VibeStream tracks.
    Each type maps to a different Kafka partition weight.
    LIKE and IMPRESSION are highest volume (~70% of traffic).
    """
    LIKE        = "like"
    SHARE       = "share"
    COMMENT     = "comment"
    IMPRESSION  = "impression"
    WATCH       = "watch"           # Video-specific: partial or full watch
    SAVE        = "save"            # Save to collection


class ContentType(str, Enum):
    VIDEO       = "video"
    IMAGE       = "image"
    TEXT        = "text"
    REEL        = "reel"            # Short-form video (highest engagement)


class DeviceType(str, Enum):
    MOBILE_IOS      = "mobile_ios"
    MOBILE_ANDROID  = "mobile_android"
    DESKTOP         = "desktop"
    TABLET          = "tablet"


@dataclass
class EngagementEvent:
    """
    Canonical schema for a VibeStream engagement event.

    Partitioning Strategy (Kafka):
    - We partition by user_id (not content_id) to ensure all events from the same user land in the same partition.
    - This enables stateful per-user aggregations in Spark without expensive shuffles across partitions.
    - Trade-off: hot partitions possible if a celebrity user goes viral.
      Production mitigation: salted partition keys for top 1% users.
    """
    event_id:       str         # UUID — deduplication key
    event_type:     str         # EventType enum value
    user_id:        str         # Anonymous user identifier
    content_id:     str         # Post/video/image being interacted with
    content_type:   str         # ContentType enum value
    creator_id:     str         # Content author's user_id
    device_type:    str         # DeviceType enum value
    platform:       str         # "ios_app" | "android_app" | "web"
    event_ts:       str         # ISO 8601 UTC timestamp (source of truth)
    ingestion_ts:   str         # When the producer sent it to Kafka
    session_id:     str         # Browser/app session identifier
    watch_duration: Optional[float] = None  # Seconds — only for WATCH events
    comment_length: Optional[int]   = None  # Chars — only for COMMENT events

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @property
    def partition_key(self) -> str:
        """Kafka partition key — routes all user events to same partition."""
        return self.user_id


def create_event(
    event_type: EventType,
    user_id: str,
    content_id: str,
    content_type: ContentType,
    creator_id: str,
    device_type: DeviceType = DeviceType.MOBILE_IOS,
    platform: str = "ios_app",
    watch_duration: Optional[float] = None,
    comment_length: Optional[int] = None,
) -> EngagementEvent:
    """Factory function to create a validated EngagementEvent."""
    now = datetime.now(timezone.utc).isoformat()
    return EngagementEvent(
        event_id        = str(uuid.uuid4()),
        event_type      = event_type.value,
        user_id         = user_id,
        content_id      = content_id,
        content_type    = content_type.value,
        creator_id      = creator_id,
        device_type     = device_type.value,
        platform        = platform,
        event_ts        = now,
        ingestion_ts    = now,
        session_id      = str(uuid.uuid4()),
        watch_duration  = watch_duration,
        comment_length  = comment_length,
    )
