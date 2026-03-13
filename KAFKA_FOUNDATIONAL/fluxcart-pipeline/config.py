"""
config.py — FluxCart Kafka Pipeline Configuration
This is not run directly, just referenced by other files.
=================================================

PURPOSE:
    Single source of truth for every Kafka setting used across the pipeline.
    Every other file imports from here — producer.py, setup_topics.py,
    analytics.py, fraud.py, inventory.py all import this file.

WHY THIS EXISTS AS A SEPARATE FILE:
    In a real pipeline, you never hardcode Kafka addresses or topic names
    in multiple places. If your Kafka cluster moves to a new server, you
    change ONE line here and every component picks it up automatically.
    If a topic is renamed, you change it here — not in 5 different files.

TEACHING NOTE:
    Read this file carefully before writing any other file.
    Every tuning knob for producers and consumers is explained here.
    Understanding these settings is understanding how Kafka behaves.
"""


# =============================================================================
# BROKER CONNECTION
# =============================================================================

# The address(es) your Python code uses to first connect to the Kafka cluster.
# Kafka returns metadata about all brokers after the first connection.

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"


# =============================================================================
# TOPIC NAMES
# =============================================================================
# All topic names defined as constants in one place.

TOPIC_USER_BEHAVIOR = "fluxcart.user.behavior"   # browse, search, add-to-cart events
TOPIC_ORDERS = "fluxcart.orders"                 # order lifecycle events
TOPIC_PAYMENTS = "fluxcart.payments"             # payment lifecycle events
TOPIC_FRAUD_ALERTS = "fluxcart.fraud.alerts"     # fraud detections
TOPIC_DEAD_LETTER = "fluxcart.dead.letter"       # unprocessable events


# =============================================================================
# TOPIC CONFIGURATION
# =============================================================================

TOPIC_CONFIGS = {

    TOPIC_USER_BEHAVIOR: {
        "num_partitions": 6,
        "replication_factor": 3,
        "retention_ms": 30 * 24 * 60 * 60 * 1000,   # 30 days
    },

    TOPIC_ORDERS: {
        "num_partitions": 3,
        "replication_factor": 3,
        "retention_ms": 90 * 24 * 60 * 60 * 1000,   # 90 days
    },

    TOPIC_PAYMENTS: {
        "num_partitions": 3,
        "replication_factor": 3,
        "retention_ms": 90 * 24 * 60 * 60 * 1000,
    },

    TOPIC_FRAUD_ALERTS: {
        "num_partitions": 1,
        "replication_factor": 3,
        "retention_ms": 30 * 24 * 60 * 60 * 1000,
    },

    TOPIC_DEAD_LETTER: {
        "num_partitions": 1,
        "replication_factor": 3,
        "retention_ms": 30 * 24 * 60 * 60 * 1000,
    },
}


# =============================================================================
# PRODUCER CONFIGURATION
# =============================================================================

PRODUCER_CONFIG = {

    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    "client.id": "fluxcart-event-simulator",

    # Acknowledgement mode
    "acks": "all",

    # Retry settings
    "retries": 5,
    "retry.backoff.ms": 300,

    # Batching
    "linger.ms": 20,
    "batch.size": 32768,

    # Compression
    "compression.type": "snappy",

    # Prevent duplicate messages
    "enable.idempotence": True,
}


# =============================================================================
# CONSUMER CONFIGURATION FACTORY
# =============================================================================

def consumer_config(group_id: str, offset_reset: str = "earliest") -> dict:
    """
    Generate a Kafka consumer configuration dictionary.

    Args:
        group_id: Unique identifier for this consumer group.

        offset_reset:
            "earliest" → read from beginning of topic
            "latest"   → read only new events

    Returns:
        dict: Consumer configuration
    """

    return {

        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

        "group.id": group_id,

        "auto.offset.reset": offset_reset,

        # Disable auto commit (manual commit safer)
        "enable.auto.commit": False,

        # Consumer health settings
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,

        # Max processing time before Kafka triggers rebalance
        "max.poll.interval.ms": 300000,
    }


# =============================================================================
# CONSUMER GROUP IDs
# =============================================================================

GROUP_ANALYTICS = "fluxcart-analytics"     # reads behavior + orders
GROUP_FRAUD = "fluxcart-fraud"             # reads payments + orders
GROUP_INVENTORY = "fluxcart-inventory"     # reads orders only


# =============================================================================
# PIPELINE SETTINGS
# =============================================================================

# Producer event generation rate
EVENTS_PER_SECOND = 100

# Consumer commit frequency
CONSUMER_COMMIT_BATCH_SIZE = 100

# Reporting window for analytics / fraud / inventory
CONSUMER_WINDOW_SECONDS = 10


# =============================================================================
# DEV NOTES
# =============================================================================

# Verify config
# grep "KAFKA_BOOTSTRAP_SERVERS" config.py

# Create topics
# python setup_topics.py

# If broker-2 error occurs during topic creation, ignore for local demo.