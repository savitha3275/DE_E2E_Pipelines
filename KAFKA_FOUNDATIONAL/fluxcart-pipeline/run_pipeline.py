"""
run_pipeline.py — FluxCart Pipeline Orchestrator
=================================================

PURPOSE:
    Starts the entire FluxCart pipeline with one command.
    Runs the producer and all three consumers simultaneously
    using Python threads — one thread per component.

    Without this file, you need 4 separate terminals:
        Terminal 1 → python3 producer.py
        Terminal 2 → python3 consumers/analytics.py
        Terminal 3 → python3 consumers/fraud.py
        Terminal 4 → python3 consumers/inventory.py

    With this file, one terminal does everything:
        python3 run_pipeline.py

HOW IT WORKS:
    Python's threading module lets multiple functions run at the same time
    in the same process. Each component runs in its own thread:

        Main thread    → watches for Ctrl+C, coordinates shutdown
        Thread 1       → FluxCartProducer.run()
        Thread 2       → AnalyticsConsumer.run()
        Thread 3       → FraudConsumer.run()
        Thread 4       → InventoryConsumer.run()

    When Ctrl+C is pressed:
        1. Main thread catches the KeyboardInterrupt
        2. Calls stop() on all three consumers (sets their shutdown flags)
        3. Waits for all threads to finish (join)
        4. Every consumer commits its final batch and closes cleanly

HOW TO RUN:
    python3 run_pipeline.py

    Optional flags:
        python3 run_pipeline.py --rate 50       # 50 events/second (default 50)
        python3 run_pipeline.py --duration 300  # run for 5 minutes (default 120s)

WHAT YOU WILL SEE:
    All four components print to the same terminal.
    Each line is prefixed with its component name so you can tell them apart:
        [fluxcart-analytics]  ...
        [fluxcart-fraud]      ...
        [fluxcart-inventory]  ...
        Producer lines have no prefix — they show rate/sent/failed

REQUIRES:
    - Kafka cluster running     (docker compose up -d)
    - Topics created            (python3 setup_topics.py)
    - pip install confluent-kafka
"""

# =============================================================================
# IMPORTS
# =============================================================================

import sys
import os

# Add project root to Python path so all imports work correctly
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import threading
# threading.Thread — runs a function in a separate thread (parallel execution)
# threading.Event  — a flag used to signal threads to stop

import time
# time.sleep() — used to give components a moment to start up before
# the producer starts sending events

import argparse
# argparse — parses command line arguments like --rate and --duration
# Lets students customize the pipeline without editing the source code

from producer import FluxCartProducer
# The producer class — generates and sends events to Kafka

from consumers.analytics import AnalyticsConsumer
# Reads behavior + orders, prints analytics window reports

from consumers.fraud import FraudConsumer
# Reads payments + orders, detects fraud, publishes alerts

from consumers.inventory import InventoryConsumer
# Reads orders, tracks stock movement and warehouse activity


# =============================================================================
# PIPELINE ORCHESTRATOR
# =============================================================================

class FluxCartPipeline:
    """
    Orchestrates the entire FluxCart pipeline.

    Creates one producer and three consumers.
    Runs each in its own thread simultaneously.
    Handles graceful shutdown when Ctrl+C is pressed.
    """

    def __init__(self, events_per_second: int = 50, duration_seconds: int = 120):
        """
        Initialize the pipeline with all components.

        Args:
            events_per_second: how fast the producer generates events
            duration_seconds:  how long the producer runs before stopping
                               consumers keep running until Ctrl+C
        """

        self.events_per_second = events_per_second
        self.duration_seconds  = duration_seconds

        # ── Create all components ─────────────────────────────────────────
        # Each component is created here but not started yet.
        # Starting happens in run() when we launch the threads.

        print("=" * 62)
        print("  FluxCart Pipeline — Initializing")
        print("=" * 62)
        print()

        print("Creating producer...")
        self.producer = FluxCartProducer()
        # FluxCartProducer connects to Kafka and sets up internal buffers

        print("Creating analytics consumer...")
        self.analytics = AnalyticsConsumer()
        # Subscribes to fluxcart.user.behavior + fluxcart.orders

        print("Creating fraud consumer...")
        self.fraud = FraudConsumer()
        # Subscribes to fluxcart.payments + fluxcart.orders

        print("Creating inventory consumer...")
        self.inventory = InventoryConsumer()
        # Subscribes to fluxcart.orders

        print()
        print("=" * 62)
        print(f"  All components ready.")
        print(f"  Rate     : {events_per_second} events/second")
        print(f"  Duration : {duration_seconds} seconds")
        print(f"  Press Ctrl+C to stop early.")
        print("=" * 62)
        print()

        # ── Thread list ───────────────────────────────────────────────────
        # We store all threads here so we can wait for them to finish
        # during shutdown (thread.join())
        self.threads = []

    def _run_producer(self):
        """
        Target function for the producer thread.

        Runs the producer for duration_seconds then stops naturally.
        After the producer finishes, consumers keep running until Ctrl+C.

        This function runs in Thread 1.
        """
        try:
            self.producer.run(
                events_per_second = self.events_per_second,
                duration_seconds  = self.duration_seconds,
            )
            # run() blocks until duration_seconds have elapsed or Ctrl+C
            print("\n[Pipeline] Producer finished. Consumers still running.")
            print("[Pipeline] Press Ctrl+C to stop consumers.\n")

        except Exception as e:
            print(f"[Pipeline] Producer error: {e}")

    def _run_consumer(self, consumer, name: str):
        """
        Target function for each consumer thread.

        Wraps consumer.run() with error handling so a consumer crash
        does not silently kill the thread without logging.

        This function runs in Thread 2, 3, and 4.

        Args:
            consumer: the consumer instance (analytics, fraud, or inventory)
            name:     human readable name for error messages
        """
        try:
            consumer.run()
            # run() blocks forever until consumer.stop() is called
            # or Ctrl+C is pressed

        except Exception as e:
            print(f"[Pipeline] {name} consumer error: {e}")

    def run(self):
        """
        Start all threads and wait for shutdown.

        Steps:
            1. Start the three consumer threads
            2. Wait 2 seconds for consumers to connect and subscribe
            3. Start the producer thread
            4. Wait for Ctrl+C
            5. On Ctrl+C: stop all consumers, wait for threads to finish
        """

        # ── Step 1: Start consumer threads ───────────────────────────────
        # Start consumers BEFORE the producer.
        # Why? If the producer starts first, it may send events before
        # consumers are subscribed. Those events would still be in Kafka
        # (consumers read from "earliest") but it is cleaner to have
        # consumers ready before events start flowing.

        consumer_configs = [
            (self.analytics, "analytics"),
            (self.fraud,     "fraud"),
            (self.inventory, "inventory"),
        ]

        for consumer, name in consumer_configs:
            # daemon=True means: if the main thread exits, kill this thread too.
            # This prevents zombie threads if something goes wrong during shutdown.
            thread = threading.Thread(
                target = self._run_consumer,
                args   = (consumer, name),
                daemon = True,
                name   = f"thread-{name}",
            )
            thread.start()
            # .start() launches the thread — _run_consumer() begins running
            # in the background immediately

            self.threads.append(thread)
            print(f"[Pipeline] {name} consumer thread started")

        # ── Step 2: Brief startup pause ───────────────────────────────────
        print(f"\n[Pipeline] Waiting for consumers to connect...")
        time.sleep(3)
        # Give consumers 3 seconds to connect to Kafka and get partition
        # assignments before the producer starts sending events.
        # Without this pause, the first few hundred events might arrive
        # before consumers have finished subscribing.

        # ── Step 3: Start producer thread ────────────────────────────────
        producer_thread = threading.Thread(
            target = self._run_producer,
            daemon = True,
            name   = "thread-producer",
        )
        producer_thread.start()
        self.threads.append(producer_thread)
        print(f"[Pipeline] Producer thread started")
        print(f"[Pipeline] Pipeline is running. Press Ctrl+C to stop.\n")

        # ── Step 4: Wait for Ctrl+C ───────────────────────────────────────
        try:
            # Keep the main thread alive while all other threads run.
            # We loop and sleep because if the main thread exits,
            # all daemon threads are killed immediately.
            while True:
                time.sleep(1)
                # Sleep 1 second at a time so Ctrl+C is caught quickly.
                # If we slept for 60 seconds, the pipeline would not
                # respond to Ctrl+C for up to 60 seconds.

        except KeyboardInterrupt:
            # Ctrl+C pressed — begin graceful shutdown
            print("\n\n[Pipeline] Ctrl+C received — shutting down...")
            self._shutdown()

    def _shutdown(self):
        """
        Gracefully stop all consumers and wait for threads to finish.

        Steps:
            1. Call stop() on each consumer (sets their shutdown flags)
            2. Wait for each thread to finish (join)
            3. Print final pipeline summary
        """

        print("[Pipeline] Stopping all consumers...")

        # Call stop() on each consumer
        # stop() sets the consumer's shutdown_event flag.
        # The poll loop checks this flag at the top of every iteration
        # and exits cleanly when it is set.
        self.analytics.stop()
        self.fraud.stop()
        self.inventory.stop()

        print("[Pipeline] Waiting for threads to finish...")

        # Wait for every thread to finish before exiting
        # join() blocks until the thread's function returns
        # timeout=15 means: wait at most 15 seconds per thread
        # If a thread takes longer than 15 seconds, we move on
        for thread in self.threads:
            thread.join(timeout=15)
            # After join(), the thread has finished — consumer committed
            # its final batch, flushed dead letters, and closed connection

            if thread.is_alive():
                # Thread did not finish within 15 seconds — log it
                print(f"[Pipeline] Warning: {thread.name} did not stop cleanly")

        print()
        print("=" * 62)
        print("  FluxCart Pipeline — Stopped")
        print("=" * 62)
        print(f"  Analytics processed : {self.analytics.total_processed}")
        print(f"  Fraud processed     : {self.fraud.total_processed}")
        print(f"  Inventory processed : {self.inventory.total_processed}")
        print(f"  Fraud alerts raised : {len(self.fraud.alerts_this_window)}")
        print("=" * 62)


# =============================================================================
# ARGUMENT PARSER
# =============================================================================

def parse_args():
    """
    Parse optional command line arguments.

    Lets students customize the pipeline without editing source code:
        python3 run_pipeline.py --rate 100 --duration 300

    Returns:
        argparse.Namespace with .rate and .duration attributes
    """

    parser = argparse.ArgumentParser(
        description="Run the FluxCart Kafka pipeline"
    )

    parser.add_argument(
        "--rate",
        type    = int,
        default = 50,
        help    = "Events per second (default: 50)"
        # 50/sec = 3000/minute — visible and not overwhelming for a laptop
    )

    parser.add_argument(
        "--duration",
        type    = int,
        default = 120,
        help    = "Producer duration in seconds (default: 120)"
        # 120 seconds = 2 minutes — enough to see meaningful data
        # Consumers keep running after producer stops until Ctrl+C
    )

    return parser.parse_args()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """
    Parse arguments, create pipeline, and run it.
    """

    args = parse_args()
    # args.rate     = events per second (default 50)
    # args.duration = producer run time in seconds (default 120)

    pipeline = FluxCartPipeline(
        events_per_second = args.rate,
        duration_seconds  = args.duration,
    )

    pipeline.run()
    # run() blocks here until Ctrl+C is pressed
    # then _shutdown() is called and the program exits


if __name__ == "__main__":
    main()