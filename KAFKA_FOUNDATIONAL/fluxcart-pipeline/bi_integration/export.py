"""
export.py — FluxCart Power BI CSV Exporter
==========================================

In order to run this file, 
first execute run_pipeline in 1 terminal , then export.py code in 2 terminal
and then streamlit run streamlit_pipeline.py in 3 terminal

PURPOSE:
    Reads live data from Kafka topics and writes CSV files every 60 seconds.
    Power BI (browser version at app.powerbi.com) reads these CSV files
    to build live dashboards and visualizations.

    This bridges Kafka (real-time streaming) with Power BI (file-based BI tool).

HOW IT WORKS:
    This file runs three internal consumers — one per topic group.
    Every EXPORT_INTERVAL_SECONDS (60), it flushes accumulated data
    into three CSV files inside the powerbi/data/ folder.

    Power BI connects to these files. You manually refresh Power BI
    (or set auto-refresh) to see the latest data.

FILES WRITTEN:
    powerbi/data/analytics_summary.csv   — behavior + order aggregates per window
    powerbi/data/fraud_alerts.csv        — one row per fraud alert detected
    powerbi/data/inventory_summary.csv   — stock movement + warehouse data per window

POWER BI SETUP (after this script is running):
    1. Go to app.powerbi.com — sign in with any Microsoft account (free)
    2. Click "Get Data" → "Text/CSV"
    3. Upload analytics_summary.csv, fraud_alerts.csv, inventory_summary.csv
    4. Build visuals:
         - Line chart: revenue over time (from analytics_summary.csv)
         - Bar chart:  top products (from analytics_summary.csv)
         - Table:      fraud alerts (from fraud_alerts.csv)
         - Bar chart:  warehouse activity (from inventory_summary.csv)
    5. To refresh: re-upload the CSV files after this script has run a few cycles

HOW TO RUN:
    # Terminal 1 — make sure producer is running
    python3 producer.py

    # Terminal 2 — start the exporter
    python3 powerbi/export.py

    CSV files are written every 60 seconds to powerbi/data/

REQUIRES:
    - Kafka cluster running       (docker compose up -d)
    - Topics created              (python3 setup_topics.py)
    - Producer running            (python3 producer.py)
    - pip install confluent-kafka
"""

# =============================================================================
# IMPORTS
# =============================================================================

import sys
import os

# Add project root to Python path so we can import config and consumers
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv          # for writing CSV files — Power BI reads CSV natively
import time         # for the export interval timer
import threading    # for running consumers in background threads
from datetime import datetime   # for timestamps on each exported row
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
# We use our own lightweight consumers here — not the BaseConsumer subclasses.
# We need raw access to the data so we can write it to CSV ourselves.

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_USER_BEHAVIOR,    # "fluxcart.user.behavior"
    TOPIC_ORDERS,           # "fluxcart.orders"
    TOPIC_PAYMENTS,         # "fluxcart.payments"
    TOPIC_FRAUD_ALERTS,     # "fluxcart.fraud.alerts"
    consumer_config,
)

import json   # for deserializing Kafka messages (they are stored as JSON bytes)


# =============================================================================
# PATHS
# =============================================================================

# Where this file lives: fluxcart-pipeline/powerbi/export.py
# We write CSV files to: fluxcart-pipeline/powerbi/data/

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
# __file__ = the path of THIS file (export.py)
# dirname  = the folder containing this file (powerbi/)

DATA_DIR  = os.path.join(BASE_DIR, "data")
# DATA_DIR = powerbi/data/

os.makedirs(DATA_DIR, exist_ok=True)
# Create the data/ folder if it does not exist yet.
# exist_ok=True means: do not raise an error if it already exists.

# Full paths for each CSV file
ANALYTICS_CSV  = os.path.join(DATA_DIR, "analytics_summary.csv")
FRAUD_CSV      = os.path.join(DATA_DIR, "fraud_alerts.csv")
INVENTORY_CSV  = os.path.join(DATA_DIR, "inventory_summary.csv")


# =============================================================================
# EXPORT INTERVAL
# =============================================================================

EXPORT_INTERVAL_SECONDS = 60
# How often to write CSV files.
# 60 seconds = Power BI data is at most 1 minute stale.
# Lower this for more frequent updates (e.g. 30) or raise it to reduce I/O.


# =============================================================================
# SHARED STATE
# =============================================================================
# These lists accumulate rows as events are consumed.
# Every EXPORT_INTERVAL_SECONDS they are written to CSV and cleared.
# A threading.Lock protects them from being written and read at the same time.

analytics_rows  = []   # one row per 10-second window from analytics consumer
fraud_rows      = []   # one row per fraud alert detected
inventory_rows  = []   # one row per 10-second window from inventory consumer

state_lock = threading.Lock()
# threading.Lock() = a mutex (mutual exclusion lock).
# When the exporter thread writes to CSV, it acquires the lock.
# The consumer threads cannot modify the lists during that time.
# This prevents corrupt CSV rows from partial writes.


# =============================================================================
# ANALYTICS EXPORTER CONSUMER
# =============================================================================

class AnalyticsExporter:
    """
    Lightweight consumer that reads behavior + order events
    and accumulates window summaries for CSV export.

    This is NOT a subclass of BaseConsumer — it is a simpler,
    self-contained consumer focused only on building CSV rows.
    """

    WINDOW_SECONDS = 10
    # How many seconds to accumulate before building a summary row.
    # Matches CONSUMER_WINDOW_SECONDS from config.py.

    GROUP_ID = "fluxcart-powerbi-analytics"
    # Separate group ID from the main analytics consumer.
    # This consumer reads the SAME topics independently —
    # Kafka delivers all messages to both groups.

    def __init__(self):
        """Connect to Kafka and subscribe to behavior + orders topics."""

        cfg = consumer_config(self.GROUP_ID, offset_reset="earliest")
        self.consumer = Consumer(cfg)
        self.consumer.subscribe([TOPIC_USER_BEHAVIOR, TOPIC_ORDERS])

        self._reset_window()
        self.window_start = time.time()
        self.running = True

    def _reset_window(self):
        """Reset all window accumulators."""
        self.behavior_count  = 0
        self.action_counts   = defaultdict(int)
        self.product_counts  = defaultdict(int)
        self.category_counts = defaultdict(int)
        self.order_count     = 0
        self.order_totals    = []
        self.status_counts   = defaultdict(int)

    def run(self):
        """
        Poll loop — reads events and accumulates state.
        Every WINDOW_SECONDS, builds a CSV row and adds it to analytics_rows.
        """

        while self.running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                # No message this poll — check if window has elapsed
                pass

            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[export-analytics] Error: {msg.error()}")

            else:
                # Deserialize the message bytes to a Python dict
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    topic = msg.topic()

                    if topic == TOPIC_USER_BEHAVIOR:
                        self.behavior_count += 1
                        self.action_counts[event.get("action", "unknown")] += 1
                        self.product_counts[event.get("product_id", "unknown")] += 1
                        self.category_counts[event.get("category", "unknown")] += 1

                    elif topic == TOPIC_ORDERS:
                        self.order_count += 1
                        self.order_totals.append(event.get("order_total", 0))
                        self.status_counts[event.get("status", "unknown")] += 1

                except Exception as e:
                    print(f"[export-analytics] Deserialize error: {e}")

            # ── Check if window has elapsed ───────────────────────────────
            if time.time() - self.window_start >= self.WINDOW_SECONDS:
                self._flush_window()

        self.consumer.close()

    def _flush_window(self):
        """
        Build a summary CSV row from the current window and
        append it to the shared analytics_rows list.
        """

        now = datetime.utcnow().isoformat()

        # Find top product and category for this window
        top_product  = max(self.product_counts,  key=self.product_counts.get)  if self.product_counts  else "none"
        top_category = max(self.category_counts, key=self.category_counts.get) if self.category_counts else "none"
        top_action   = max(self.action_counts,   key=self.action_counts.get)   if self.action_counts   else "none"

        avg_order_value = (
            sum(self.order_totals) / len(self.order_totals)
            if self.order_totals else 0
        )
        total_revenue = sum(self.order_totals) if self.order_totals else 0

        row = {
            "timestamp":         now,
            "behavior_events":   self.behavior_count,
            "top_action":        top_action,
            "top_product":       top_product,
            "top_category":      top_category,
            "order_events":      self.order_count,
            "avg_order_value":   round(avg_order_value, 2),
            "total_revenue":     round(total_revenue, 2),
            "orders_placed":     self.status_counts.get("placed", 0),
            "orders_delivered":  self.status_counts.get("delivered", 0),
            "orders_cancelled":  self.status_counts.get("cancelled", 0),
        }
        # Each key becomes a column in the CSV file.
        # Power BI will use these column names in its data model.

        with state_lock:
            analytics_rows.append(row)
            # Acquire the lock before modifying the shared list.
            # The exporter thread may be reading this list at the same time.

        self._reset_window()
        self.window_start = time.time()

    def stop(self):
        """Signal the poll loop to stop."""
        self.running = False


# =============================================================================
# FRAUD EXPORTER CONSUMER
# =============================================================================

class FraudExporter:
    """
    Reads from fluxcart.fraud.alerts — the topic that FraudConsumer writes to.
    Every alert written by FraudConsumer becomes one row in fraud_alerts.csv.

    We read from the ALERTS topic (not payments) because:
    - FraudConsumer already applied the rules and wrote clean alert events
    - We do not need to re-apply fraud logic here
    - Each alert row is already structured: rule, severity, user_id, amount
    """

    GROUP_ID = "fluxcart-powerbi-fraud"

    def __init__(self):
        """Connect to Kafka and subscribe to the fraud alerts topic."""
        cfg = consumer_config(self.GROUP_ID, offset_reset="earliest")
        self.consumer = Consumer(cfg)
        self.consumer.subscribe([TOPIC_FRAUD_ALERTS])
        self.running = True

    def run(self):
        """
        Poll loop — reads fraud alert events.
        Each alert becomes one row in fraud_alerts.csv immediately.
        No windowing needed — we want every alert as its own row.
        """

        while self.running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                continue

            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[export-fraud] Error: {msg.error()}")
                continue

            try:
                alert = json.loads(msg.value().decode("utf-8"))

                row = {
                    "timestamp":   datetime.utcnow().isoformat(),
                    "alert_id":    alert.get("alert_id", ""),
                    "rule":        alert.get("rule", ""),
                    "severity":    alert.get("severity", ""),
                    "user_id":     alert.get("user_id", ""),
                    "payment_id":  alert.get("payment_id", ""),
                    "order_id":    alert.get("order_id", ""),
                    "amount":      alert.get("amount", 0),
                    "detail":      alert.get("detail", ""),
                }
                # Each row = one fraud alert = one row in Power BI table

                with state_lock:
                    fraud_rows.append(row)

            except Exception as e:
                print(f"[export-fraud] Deserialize error: {e}")

        self.consumer.close()

    def stop(self):
        """Signal the poll loop to stop."""
        self.running = False


# =============================================================================
# INVENTORY EXPORTER CONSUMER
# =============================================================================

class InventoryExporter:
    """
    Reads order events and builds inventory window summaries for CSV export.
    Same logic as InventoryConsumer but outputs to CSV instead of terminal.
    """

    WINDOW_SECONDS = 10
    GROUP_ID = "fluxcart-powerbi-inventory"

    def __init__(self):
        """Connect to Kafka and subscribe to orders topic."""
        cfg = consumer_config(self.GROUP_ID, offset_reset="earliest")
        self.consumer = Consumer(cfg)
        self.consumer.subscribe([TOPIC_ORDERS])
        self._reset_window()
        self.window_start = time.time()
        self.running = True

    def _reset_window(self):
        """Reset all window accumulators."""
        self.order_count      = 0
        self.units_ordered    = 0
        self.units_delivered  = 0
        self.units_cancelled  = 0
        self.warehouse_counts = defaultdict(int)
        self.product_units    = defaultdict(int)
        self.category_units   = defaultdict(int)
        self.status_counts    = defaultdict(int)

    def run(self):
        """Poll loop — reads order events and accumulates inventory state."""

        while self.running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                pass

            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[export-inventory] Error: {msg.error()}")

            else:
                try:
                    event    = json.loads(msg.value().decode("utf-8"))
                    status   = event.get("status", "")
                    quantity = event.get("quantity", 1)

                    self.order_count += 1
                    self.status_counts[status] += 1

                    if status == "placed":
                        self.units_ordered += quantity
                    elif status == "delivered":
                        self.units_delivered += quantity
                    elif status == "cancelled":
                        self.units_cancelled += quantity

                    self.warehouse_counts[event.get("warehouse_id", "unknown")] += 1
                    self.product_units[event.get("product_id", "unknown")]       += quantity
                    self.category_units[event.get("category", "unknown")]        += quantity

                except Exception as e:
                    print(f"[export-inventory] Deserialize error: {e}")

            if time.time() - self.window_start >= self.WINDOW_SECONDS:
                self._flush_window()

        self.consumer.close()

    def _flush_window(self):
        """Build a summary CSV row from the current window."""

        now = datetime.utcnow().isoformat()

        top_warehouse = max(self.warehouse_counts, key=self.warehouse_counts.get) if self.warehouse_counts else "none"
        top_product   = max(self.product_units,   key=self.product_units.get)    if self.product_units   else "none"
        top_category  = max(self.category_units,  key=self.category_units.get)   if self.category_units  else "none"

        fulfillment_rate = (
            round((self.units_delivered / self.units_ordered) * 100, 1)
            if self.units_ordered > 0 else 0
        )

        row = {
            "timestamp":         now,
            "order_events":      self.order_count,
            "units_ordered":     self.units_ordered,
            "units_delivered":   self.units_delivered,
            "units_cancelled":   self.units_cancelled,
            "fulfillment_rate":  fulfillment_rate,
            "top_warehouse":     top_warehouse,
            "top_product":       top_product,
            "top_category":      top_category,
            "orders_placed":     self.status_counts.get("placed", 0),
            "orders_delivered":  self.status_counts.get("delivered", 0),
            "orders_cancelled":  self.status_counts.get("cancelled", 0),
        }

        with state_lock:
            inventory_rows.append(row)

        self._reset_window()
        self.window_start = time.time()

    def stop(self):
        """Signal the poll loop to stop."""
        self.running = False


# =============================================================================
# CSV WRITER
# =============================================================================

def write_csv(filepath: str, rows: list, fieldnames: list):
    """
    Append a batch of new rows to a CSV file.

    APPEND MODE — how it works:
        - First cycle: file does not exist yet → create it with header + rows
        - Every cycle after: file exists → open in append mode, add rows only
          (no header written again — Power BI only wants one header row at the top)

    This means the CSV file grows every 60 seconds.
    Power BI reads the full growing file on every refresh and sees the
    complete history from the moment the exporter started.

    Args:
        filepath:   full path to the CSV file
        rows:       NEW rows from this cycle only — added to the bottom
        fieldnames: list of column names in order
    """

    if not rows:
        # Nothing new this cycle — do not touch the file
        # The existing file stays intact with all previous rows
        return

    file_exists = os.path.isfile(filepath)
    # Check if the file already exists BEFORE opening it.
    # os.path.isfile() returns True if the file exists, False if not.
    # We use this to decide whether to write the header row.

    with open(filepath, "a", newline="") as f:
        # "a" = append mode — opens the file and positions the write
        # cursor at the END of the file. Existing content is never touched.
        # If the file does not exist yet, "a" creates it automatically.
        # newline="" prevents double line breaks on Windows.

        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()
            # Write column names ONLY on the very first write.
            # If we wrote the header every cycle, Power BI would see
            # "timestamp,behavior_events,..." as a data row every 60s.

        writer.writerows(rows)
        # Append only the NEW rows from this cycle to the bottom of the file.
        # Previous rows are untouched — they stay exactly where they are.


# =============================================================================
# COLUMN DEFINITIONS
# =============================================================================
# These define the column order in each CSV file.
# Must match the keys in the row dicts built by each exporter.

ANALYTICS_FIELDS = [
    "timestamp", "behavior_events", "top_action", "top_product",
    "top_category", "order_events", "avg_order_value", "total_revenue",
    "orders_placed", "orders_delivered", "orders_cancelled",
]

FRAUD_FIELDS = [
    "timestamp", "alert_id", "rule", "severity",
    "user_id", "payment_id", "order_id", "amount", "detail",
]

INVENTORY_FIELDS = [
    "timestamp", "order_events", "units_ordered", "units_delivered",
    "units_cancelled", "fulfillment_rate", "top_warehouse",
    "top_product", "top_category", "orders_placed",
    "orders_delivered", "orders_cancelled",
]


# =============================================================================
# EXPORT LOOP
# =============================================================================

def export_loop(exporters: list):
    """
    Runs in the main thread.
    Every EXPORT_INTERVAL_SECONDS, writes all accumulated rows to CSV files.

    Args:
        exporters: list of (exporter instance, name) — used to call stop() on shutdown
    """

    cycle = 0
    # Track how many export cycles have run — printed in the status line

    print(f"\n[export] Writing CSV files every {EXPORT_INTERVAL_SECONDS}s")
    print(f"[export] Files: {DATA_DIR}/")
    print(f"[export] Press Ctrl+C to stop\n")

    try:
        while True:
            time.sleep(EXPORT_INTERVAL_SECONDS)
            # Wait for the export interval before writing

            cycle += 1
            now = datetime.utcnow().strftime("%H:%M:%S")

            # APPEND MODE:
            # Snapshot the new rows accumulated since the last cycle.
            # Then clear the shared lists so next cycle starts fresh.
            # The CSV file itself is never cleared — rows only ever get added.
            with state_lock:
                a_rows = list(analytics_rows)
                f_rows = list(fraud_rows)
                i_rows = list(inventory_rows)

                analytics_rows.clear()
                fraud_rows.clear()
                inventory_rows.clear()
                # We clear the IN-MEMORY lists after snapshotting.
                # This keeps memory usage flat — we do not accumulate
                # rows in RAM forever, only in the CSV file on disk.
                # The CSV file is the permanent record. RAM is just the
                # staging area between Kafka and the CSV.

            # Append the new rows to each CSV file
            # write_csv() handles the header-on-first-write logic
            write_csv(ANALYTICS_CSV, a_rows, ANALYTICS_FIELDS)
            write_csv(FRAUD_CSV,     f_rows, FRAUD_FIELDS)
            write_csv(INVENTORY_CSV, i_rows, INVENTORY_FIELDS)

            # Count total rows in each file for the status line
            # so you can see the file growing over time
            try:
                with open(ANALYTICS_CSV) as f_:
                    total_analytics = sum(1 for _ in f_) - 1  # subtract header
                with open(INVENTORY_CSV) as f_:
                    total_inventory = sum(1 for _ in f_) - 1
                total_fraud = sum(1 for _ in open(FRAUD_CSV)) - 1 if os.path.exists(FRAUD_CSV) else 0
            except Exception:
                total_analytics = total_inventory = total_fraud = "?"

            print(
                f"[export] Cycle {cycle:03d} @ {now} — "
                f"added: analytics+{len(a_rows)}  fraud+{len(f_rows)}  inventory+{len(i_rows)}  "
                f"| total in file: analytics={total_analytics}  fraud={total_fraud}  inventory={total_inventory}"
            )
            # Two numbers per file:
            # "added" = new rows written THIS cycle
            # "total" = cumulative rows in the CSV file so far
            # You will see "total" grow every cycle — that is append mode working

    except KeyboardInterrupt:
        print("\n[export] Stopping...")
        for exporter, name in exporters:
            exporter.stop()
            print(f"[export] Stopped {name}")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """
    Create all exporters, start their threads, run the export loop.
    """

    print("=" * 62)
    print("  FluxCart Power BI Exporter")
    print("=" * 62)

    # Create all three exporter consumers
    analytics_exp  = AnalyticsExporter()
    fraud_exp      = FraudExporter()
    inventory_exp  = InventoryExporter()

    exporters = [
        (analytics_exp,  "analytics"),
        (fraud_exp,      "fraud"),
        (inventory_exp,  "inventory"),
    ]

    # Start each exporter in its own background thread
    threads = []
    for exporter, name in exporters:
        thread = threading.Thread(
            target = exporter.run,
            daemon = True,
            name   = f"exporter-{name}",
        )
        thread.start()
        threads.append(thread)
        print(f"  ✅  {name} exporter started")

    print()
    print(f"  Data directory : {DATA_DIR}")
    print(f"  Export interval: every {EXPORT_INTERVAL_SECONDS} seconds")
    print("=" * 62)

    # Run the export loop in the main thread
    # This blocks until Ctrl+C
    export_loop(exporters)

    # Wait for all exporter threads to finish
    for thread in threads:
        thread.join(timeout=10)

    print("[export] All done.")


if __name__ == "__main__":
    main()