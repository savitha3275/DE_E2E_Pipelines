"""
producer.py — FluxCart Event Simulator
=======================================

PURPOSE:
    Simulates the entire FluxCart platform by generating realistic events
    and publishing them to Kafka topics at a configurable rate.

    In a real system, these events would come from actual user actions —
    a user clicking "Add to Cart" on the website, a payment being processed,
    an order being confirmed. Here we generate them with realistic fake data
    so we have something flowing through the pipeline to teach with.

HOW TO RUN:
    python3 producer.py

WHAT IT DOES:
    Generates 3 event types in a realistic mix:
        65% UserBehaviorEvents  → fluxcart.user.behavior
        25% OrderEvents         → fluxcart.orders
        10% PaymentEvents       → fluxcart.payments

    Runs for 120 seconds by default, generating 50 events per second.
    Prints delivery metrics every 10 seconds so you can watch
    partition distribution in real time.

REQUIRES:
    - Kafka cluster running     (docker compose up -d)
    - Topics created            (python3 setup_topics.py)
    - pip install confluent-kafka
"""

from confluent_kafka import Producer, KafkaException
# Producer      — the Kafka client that writes events to topics
# KafkaException — base exception class for all Kafka errors

from config import (
    PRODUCER_CONFIG,        # all producer settings (acks, batching, etc.)
    TOPIC_USER_BEHAVIOR,    # "fluxcart.user.behavior"
    TOPIC_ORDERS,           # "fluxcart.orders"
    TOPIC_PAYMENTS,         # "fluxcart.payments"
    EVENTS_PER_SECOND,      # how many events to generate per second (100)
)

from models import (
    UserBehaviorEvent,  # generates and serializes user behavior events
    OrderEvent,         # generates and serializes order events
    PaymentEvent,       # generates and serializes payment events
)

import time     # for rate limiting and timing
import random   # for choosing which event type to generate
import sys      # for sys.exit() on fatal errors


# =============================================================================
# METRICS TRACKER
# =============================================================================
# Tracks delivery statistics across the entire producer run.
# Updated by the delivery callback every time Kafka confirms a message.

class ProducerMetrics:
    """
    Tracks producer delivery statistics.

    Updated in real time by the delivery callback.
    Printed every 10 seconds and at the end of the run.
    """

    def __init__(self):
        self.total_sent     = 0   # messages successfully delivered to Kafka
        self.total_failed   = 0   # messages that failed delivery after all retries

        # Count messages per topic — lets us verify the 65/25/10 distribution
        self.by_topic = {
            TOPIC_USER_BEHAVIOR: 0,
            TOPIC_ORDERS:        0,
            TOPIC_PAYMENTS:      0,
        }

        # Count messages per partition per topic
        # Key format: "topic_name-partition_number"
        # e.g. "fluxcart.user.behavior-0", "fluxcart.orders-2"
        # Shows students how keys distribute across partitions
        self.by_partition = {}

        self.start_time = time.time()   # when the producer started

    def record_success(self, topic: str, partition: int):
        """Record a successful delivery."""
        self.total_sent += 1

        # Increment per-topic counter
        if topic in self.by_topic:
            self.by_topic[topic] += 1

        # Increment per-partition counter
        key = f"{topic}-P{partition}"
        self.by_partition[key] = self.by_partition.get(key, 0) + 1

    def record_failure(self):
        """Record a failed delivery."""
        self.total_failed += 1

    def print_summary(self, label: str = "METRICS"):
        """Print a formatted metrics summary."""

        elapsed = time.time() - self.start_time
        rate    = self.total_sent / elapsed if elapsed > 0 else 0

        print(f"\n{'='*55}")
        print(f"  {label}")
        print(f"{'='*55}")
        print(f"  Elapsed:       {elapsed:.1f}s")
        print(f"  Total sent:    {self.total_sent}")
        print(f"  Total failed:  {self.total_failed}")
        print(f"  Avg rate:      {rate:.1f} msgs/sec")

        # Per-topic breakdown
        print(f"\n  BY TOPIC:")
        for topic, count in self.by_topic.items():
            # Show the short name (after the last dot) for readability
            short_name = topic.split(".")[-1]
            pct = (count / self.total_sent * 100) if self.total_sent > 0 else 0
            print(f"    {short_name:<20} {count:>6}  ({pct:.1f}%)")

        # Per-partition breakdown — this is the interesting one
        # Students see how keys distribute across partitions
        print(f"\n  BY PARTITION:")
        for part_key in sorted(self.by_partition.keys()):
            count = self.by_partition[part_key]
            print(f"    {part_key:<35} {count:>6}")

        print(f"{'='*55}")


# =============================================================================
# FLUXCART PRODUCER
# =============================================================================

class FluxCartProducer:
    """
    Produces FluxCart events to Kafka topics.

    Wraps the confluent-kafka Producer with:
        - Delivery callback for tracking success/failure
        - Metrics collection per topic and per partition
        - Rate limiting to produce at a steady events/second rate
        - Graceful error handling with BufferError recovery
    """

    def __init__(self):
        """
        Initialize the Kafka producer and metrics tracker.
        """

        # Create the Kafka Producer using settings from config.py
        # This establishes the connection to the cluster.
        # Settings include: acks=all, retries=5, snappy compression, idempotence
        self.producer = Producer(PRODUCER_CONFIG)

        # Initialize metrics tracker
        self.metrics = ProducerMetrics()

        print("FluxCartProducer initialized.")
        print(f"  bootstrap.servers : {PRODUCER_CONFIG['bootstrap.servers']}")
        print(f"  acks              : {PRODUCER_CONFIG['acks']}")
        print(f"  compression       : {PRODUCER_CONFIG['compression.type']}")
        print(f"  idempotence       : {PRODUCER_CONFIG['enable.idempotence']}")
        print()

    # -------------------------------------------------------------------------
    # DELIVERY CALLBACK
    # -------------------------------------------------------------------------
    def _on_delivery(self, err, msg):
        """
        Called by Kafka for EVERY message — success or failure.

        This is an asynchronous callback. It does not run immediately
        when you call produce(). It runs when Kafka processes the result
        of the send — which happens when you call producer.poll().

        Args:
            err: None if delivery succeeded, KafkaError object if failed
            msg: the message that was delivered (or failed) contains
            msg.topic() : which topic it landed in 
            msg.partition() - which partition number
            msg.offset() - sequential position within that partition
        """

        if err is not None:
            # Delivery failed after all retries were exhausted.
            # In production: send to dead letter queue, trigger alert.
            # Here: log and count it.
            self.metrics.record_failure()
            print(f"  DELIVERY FAILED | "
                  f"topic={msg.topic()} | "
                  f"partition={msg.partition()} | "
                  f"error={err}")
        else:
            # Delivery succeeded — message is safely stored in Kafka.
            # msg.topic()     = which topic it landed in
            # msg.partition() = which partition within that topic
            # msg.offset()    = its position (offset) in that partition
            self.metrics.record_success(msg.topic(), msg.partition())

    # -------------------------------------------------------------------------
    # CORE PUBLISH METHOD
    # -------------------------------------------------------------------------
    def publish(self, topic: str, key: bytes, value: bytes):
        """
        Publish one event to a Kafka topic.

        Handles BufferError by flushing and retrying once.
        BufferError means the producer's internal memory buffer is full —
        we are producing faster than Kafka can accept. Flushing forces
        all buffered messages to be sent before continuing.

        Args:
            topic: topic name to publish to
            key:   partition key as bytes (determines which partition)
            value: event payload as bytes (the actual event data)
        """

        try:
            # produce() adds the message to the producer's internal buffer.
            # It does NOT send immediately — messages are batched for efficiency.
            # Actual sending happens in the background or when poll() is called.
            self.producer.produce(
                topic    = topic,     # which topic to write to
                key      = key,       # partition key — hash(key) % num_partitions
                value    = value,     # the event payload bytes
                callback = self._on_delivery,  # called when Kafka confirms/rejects
            )

        except BufferError:
            # The producer's internal buffer (default 32MB) is full.
            # This means we are producing faster than messages are being sent.
            # Solution: flush() — force send all buffered messages right now,
            # then retry the current message.
            print("  Buffer full — flushing before retrying...")
            self.producer.flush()    # blocks until all buffered msgs are sent

            # Retry the same message after the buffer is cleared
            self.producer.produce(
                topic    = topic,
                key      = key,
                value    = value,
                callback = self._on_delivery,
            )

        # poll(0) triggers delivery callbacks without blocking.
        # 0 means "don't wait — just process any callbacks that are ready now"
        # This is how _on_delivery gets called during the produce loop.
        # Without poll(), callbacks would only fire when flush() is called.
        self.producer.poll(0)

    # -------------------------------------------------------------------------
    # EVENT GENERATORS
    # -------------------------------------------------------------------------
    def produce_behavior_event(self):
        """
        Generate and publish one UserBehaviorEvent.

        Key   = user_id  (all events for same user → same partition)
        Topic = fluxcart.user.behavior
        """
        event = UserBehaviorEvent.generate()   # create a realistic fake event
        self.publish(
            topic = TOPIC_USER_BEHAVIOR,
            key   = event.partition_key,        # user_id as bytes
            value = event.to_bytes(),           # full event as JSON bytes
        )

    def produce_order_event(self):
        """
        Generate and publish one OrderEvent.

        Generates a realistic order status — most orders start as "placed",
        but some are already in later stages to simulate a live platform
        where orders are in various states simultaneously.

        Key   = order_id  (all status changes for same order → same partition)
        Topic = fluxcart.orders
        """

        # Weighted status distribution — mirrors a live platform
        # Most new events are fresh orders being placed
        # Some are orders moving through later lifecycle stages
        status = random.choices(
            population = ["placed", "confirmed", "packed", "shipped", "delivered", "cancelled"],
            weights    = [50,       20,          10,       10,        8,           2],
            k=1
        )[0]

        event = OrderEvent.generate(status=status)
        self.publish(
            topic = TOPIC_ORDERS,
            key   = event.partition_key,   # order_id as bytes
            value = event.to_bytes(),
        )

    def produce_payment_event(self):
        """
        Generate and publish one PaymentEvent.

        Most payments succeed. Some fail (declined cards, insufficient funds).
        A small number are refunds.

        Key   = order_id  (aligns with order events for fraud detection)
        Topic = fluxcart.payments
        """

        # Weighted payment status — mirrors real payment success rates
        status = random.choices(
            population = ["initiated", "success", "failed", "refunded"],
            weights    = [30,          55,        12,       3],
            k=1
        )[0]

        event = PaymentEvent.generate(status=status)
        self.publish(
            topic = TOPIC_PAYMENTS,
            key   = event.partition_key,   # order_id as bytes
            value = event.to_bytes(),
        )

    # -------------------------------------------------------------------------
    # MAIN RUN LOOP
    # -------------------------------------------------------------------------
    def run(self, events_per_second: int = EVENTS_PER_SECOND, duration_seconds: int = 120):
        """
        Run the producer for a fixed duration at a steady rate.

        Generates events in a realistic mix:
            65% user behavior events
            25% order events
            10% payment events

        Prints a metrics report every 10 seconds so students can watch
        partition distribution build up in real time.

        Args:
            events_per_second: target production rate (default from config.py)
            duration_seconds:  how long to run in seconds (default 120s = 2 min)
        """

        print(f"Starting producer:")
        print(f"  Rate     : {events_per_second} events/second")
        print(f"  Duration : {duration_seconds} seconds")
        print(f"  Total    : ~{events_per_second * duration_seconds} events")
        print(f"\nPress Ctrl+C to stop early.\n")

        start_time        = time.time()      # when the run started
        last_report_time  = start_time       # when we last printed metrics
        total_produced    = 0                # running count of events generated

        try:
            # Keep running until duration_seconds have elapsed
            while time.time() - start_time < duration_seconds:

                batch_start = time.time()   # start of this 1-second batch

                # Generate exactly events_per_second events in this batch
                for _ in range(events_per_second):

                    # Choose which event type to generate
                    # random.random() returns a float between 0.0 and 1.0
                    rand = random.random()

                    if rand < 0.65:
                        # 65% of events are user behavior (browsing, searching)
                        self.produce_behavior_event()

                    elif rand < 0.90:
                        # Next 25% are order events (0.65 to 0.90)
                        self.produce_order_event()

                    else:
                        # Remaining 10% are payment events (0.90 to 1.0)
                        self.produce_payment_event()

                    total_produced += 1

                # ---- Rate Limiting ----
                # Check how long this batch took.
                # If it took less than 1 second, sleep the remainder.
                # This keeps us at a steady events_per_second rate.
                # Without this, we would produce as fast as possible
                # and overwhelm the cluster.
                batch_elapsed = time.time() - batch_start
                if batch_elapsed < 1.0:
                    time.sleep(1.0 - batch_elapsed)

                # ---- Progress Report Every 10 Seconds ----
                # Print a metrics summary so students can watch the numbers grow
                if time.time() - last_report_time >= 10:
                    elapsed = time.time() - start_time
                    print(f"[{elapsed:5.0f}s] "
                          f"Produced: {total_produced:6d} | "
                          f"Sent: {self.metrics.total_sent:6d} | "
                          f"Failed: {self.metrics.total_failed:3d}")
                    last_report_time = time.time()

        except KeyboardInterrupt:
            # Student pressed Ctrl+C — stop gracefully
            print("\n\nCtrl+C received — stopping producer...")

        finally:
            # ---- ALWAYS flush before exiting ----
            # flush() blocks until ALL buffered messages have been sent
            # and all delivery callbacks have fired.
            # Without this, messages sitting in the buffer when the script
            # exits are LOST — they never reach Kafka.
            print("Flushing remaining messages...")
            self.producer.flush()
            print("Done.\n")

            # Print final metrics summary
            self.metrics.print_summary("FINAL PRODUCER METRICS")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Create and run the FluxCart producer."""

    producer = FluxCartProducer()
    producer.run(
        events_per_second = 50,    # 50/sec is visible and not overwhelming
        duration_seconds  = 120,   # run for 2 minutes
    )


if __name__ == "__main__":
    main()
    
# once this is done, run this using python3 producer.py
# if facing error : sudo nano /etc/hosts
'''
and then add
127.0.0.1   broker-1
127.0.0.1   broker-2
127.0.0.1   broker-3
and then crtl O and enter and ctrl X

and then run grep broker /etc/hosts to verify the entries are there
'''


'''
Output : 

=======================================================
  FINAL PRODUCER METRICS
=======================================================
  Elapsed:       120.4s
  Total sent:    6000
  Total failed:  0
  Avg rate:      49.8 msgs/sec

  Avg rate:      49.8 msgs/sec

  BY TOPIC:
    behavior               3892  (64.9%)
    orders                 1476  (24.6%)
    payments                632  (10.5%)

  BY PARTITION:
    fluxcart.orders-P0                     475
    behavior               3892  (64.9%)
    orders                 1476  (24.6%)
    payments                632  (10.5%)

  BY PARTITION:
    fluxcart.orders-P0                     475
    orders                 1476  (24.6%)
    payments                632  (10.5%)

  BY PARTITION:
    fluxcart.orders-P0                     475

  BY PARTITION:
    fluxcart.orders-P0                     475
    fluxcart.orders-P0                     475
    fluxcart.orders-P1                     491
    fluxcart.orders-P2                     510
    fluxcart.payments-P0                   218
    fluxcart.payments-P1                   196
    fluxcart.payments-P2                   218
    fluxcart.user.behavior-P0              373
    fluxcart.user.behavior-P1              455
    fluxcart.user.behavior-P2              820
    fluxcart.user.behavior-P3              395
    fluxcart.user.behavior-P4              733
    fluxcart.user.behavior-P5             1116
=======================================================

'''

# after this we will go to base_consumer.py