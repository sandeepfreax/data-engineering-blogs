"""
VibeStream Kafka Producer — Event Simulator

Simulates realistic social media engagement events for local dev/testing.

Realistic Traffic Distribution (based on typical social platforms):
  - IMPRESSION : 45%  (every content view)
  - LIKE       : 25%  (lightest engagement action)
  - WATCH      : 15%  (video watch events)
  - COMMENT    :  8%  (medium engagement)
  - SHARE      :  5%  (high-intent engagement)
  - SAVE       :  2%  (highest-intent engagement)

Run: python vibestream_producer.py --events-per-sec 100 --duration 600
"""

import argparse
import logging
import random
import time
import uuid

from kafka import KafkaProducer
from kafka.errors import KafkaError

from event_schema import (
    ContentType, DeviceType, EventType, create_event
)

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("VibeStreamProducer")

# ── Constants — mirrors a realistic VibeStream user base ──────────────────────
TOPIC_ENGAGEMENT  = "engagement_events"
NUM_USERS         = 5000         # Simulated DAU pool
NUM_CONTENT_ITEMS = 10000        # Active posts/videos in the feed
NUM_CREATORS      = 500          # Content creator pool

# Pre-generate stable user/content IDs so events are realistic (not random UUIDs every time)
USER_IDS     = [f"usr_{str(uuid.uuid4())[:8]}" for _ in range(NUM_USERS)]
CONTENT_IDS  = [f"cnt_{str(uuid.uuid4())[:8]}" for _ in range(NUM_CONTENT_ITEMS)]
CREATOR_IDS  = [f"crt_{str(uuid.uuid4())[:8]}" for _ in range(NUM_CREATORS)]

# Weighted event distribution
EVENT_WEIGHTS = [
    (EventType.IMPRESSION, 45),
    (EventType.LIKE,       25),
    (EventType.WATCH,      15),
    (EventType.COMMENT,     8),
    (EventType.SHARE,       5),
    (EventType.SAVE,        2),
]
EVENT_POOL    = [e for e, w in EVENT_WEIGHTS for _ in range(w)]

DEVICE_POOL   = [
    DeviceType.MOBILE_IOS,
    DeviceType.MOBILE_IOS,      # iOS over-represented (60%)
    DeviceType.MOBILE_ANDROID,
    DeviceType.DESKTOP,
    DeviceType.TABLET,
]

CONTENT_TYPE_POOL = [
    ContentType.REEL,   # 40% — highest engagement
    ContentType.VIDEO,  # 30%
    ContentType.IMAGE,  # 20%
    ContentType.TEXT,   # 10%
]


def build_producer(bootstrap_servers: str = "localhost:9092") -> KafkaProducer:
    """
    Creates a KafkaProducer with production-aligned configurations.

    Key settings:
    - acks='all': Wait for all in-sync replicas (durability over throughput)
    - compression_type='gzip': Good compression ratio with low CPU cost
    - batch_size: 64KB batches for throughput (default 16KB is too small)
    - linger_ms: Wait 10ms to fill batches (reduces requests by ~5x)
    """
    return KafkaProducer(
        bootstrap_servers   = bootstrap_servers,
        value_serializer    = lambda v: v.encode("utf-8"),
        key_serializer      = lambda k: k.encode("utf-8"),
        acks                = "all",
        retries             = 5,
        compression_type    = "gzip",       # No native lib needed, perfect for local dev
        batch_size          = 65536,       # 64KB
        linger_ms           = 10,
        request_timeout_ms  = 30000,
    )


def on_send_success(record_metadata):
    logger.debug(
        "Sent → topic=%s partition=%d offset=%d",
        record_metadata.topic, record_metadata.partition, record_metadata.offset
    )


def on_send_error(exc: KafkaError):
    logger.error("Failed to send event: %s", exc)


def generate_event():
    """Generate one realistic VibeStream engagement event."""
    event_type   = random.choice(EVENT_POOL)
    content_type = random.choice(CONTENT_TYPE_POOL)
    device_type  = random.choice(DEVICE_POOL)

    # Watch duration only makes sense for video content
    watch_duration = None
    if event_type == EventType.WATCH and content_type in (ContentType.VIDEO, ContentType.REEL):
        watch_duration = round(random.uniform(3.0, 180.0), 2)

    # Comment length only for COMMENT events
    comment_length = None
    if event_type == EventType.COMMENT:
        comment_length = random.randint(5, 280)

    return create_event(
        event_type      = event_type,
        user_id         = random.choice(USER_IDS),
        content_id      = random.choice(CONTENT_IDS),
        content_type    = content_type,
        creator_id      = random.choice(CREATOR_IDS),
        device_type     = device_type,
        watch_duration  = watch_duration,
        comment_length  = comment_length,
    )


def run_producer(
    bootstrap_servers: str,
    events_per_sec: int,
    duration_secs: int,
) -> None:
    producer      = build_producer(bootstrap_servers)
    sleep_interval = 1.0 / events_per_sec
    total_sent     = 0
    start_time     = time.time()
    end_time       = start_time + duration_secs

    logger.info(
        "Starting VibeStream producer — %d events/sec for %d seconds → %s",
        events_per_sec, duration_secs, bootstrap_servers
    )

    try:
        while time.time() < end_time:
            event = generate_event()
            (
                producer.send(
                    topic = TOPIC_ENGAGEMENT,
                    key = event.partition_key,   # Partition by user_id
                    value = event.to_json(),
                )
                .add_callback(on_send_success)
                .add_errback(on_send_error)
            )
            total_sent += 1

            if total_sent % 1000 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    "Progress: %d events sent | %.1fs elapsed | %.0f events/sec actual",
                    total_sent, elapsed, total_sent / elapsed
                )

            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        logger.info(
            "Producer finished. Total: %d events in %.1fs (%.0f events/sec)",
            total_sent, elapsed, total_sent / max(elapsed, 0.001)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VibeStream Kafka Event Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--events-per-sec", type=int, default=50)
    parser.add_argument("--duration", type=int, default=300, help="Run duration in seconds")
    args = parser.parse_args()

    run_producer(
        bootstrap_servers = args.bootstrap_servers,
        events_per_sec = args.events_per_sec,
        duration_secs = args.duration,
    )
