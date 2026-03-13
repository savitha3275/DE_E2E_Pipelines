# specific to our use case

"""
inventory.py — FluxCart Inventory Consumer
==========================================

PURPOSE:
    Reads order events from Kafka and tracks inventory in real time.
    Monitors stock movement, warehouse activity, and order lifecycle.

    This answers questions like:
        - How many units of each product were ordered this window?
        - Which warehouse is processing the most orders?
        - Which products are selling the fastest right now?
        - How many orders were placed vs delivered vs cancelled?
        - Which category is moving the most stock?

WHAT IT READS:
    fluxcart.orders — every order lifecycle event
    (placed, confirmed, packed, shipped, delivered, cancelled)

WHAT IT PRODUCES:
    Nothing. Pure consumer — reads and aggregates only.

WHY ONLY ORDERS?
    Analytics reads behavior + orders (user actions + conversions).
    Fraud reads payments + orders (financial risk).
    Inventory reads orders only — it only cares about stock movement.
    Each consumer reads exactly what it needs, nothing more.

CONSUMER GROUP:
    fluxcart-inventory

HOW TO RUN:
    python3 consumers/inventory.py

EXPECTED OUTPUT (every 10 seconds):
    [fluxcart-inventory] ══ INVENTORY WINDOW (10s) ══════════════════════════
    [fluxcart-inventory]   Orders received    : 141
    [fluxcart-inventory]   ── By Status ─────────────────────────────────────
    [fluxcart-inventory]   placed=58  confirmed=30  shipped=19  delivered=17
    [fluxcart-inventory]   cancelled=13  packed=4
    [fluxcart-inventory]   ── Stock Movement ────────────────────────────────
    [fluxcart-inventory]   Units ordered      : 187
    [fluxcart-inventory]   Units delivered    : 22
    [fluxcart-inventory]   Units cancelled    : 16
    [fluxcart-inventory]   ── Top Products ──────────────────────────────────
    [fluxcart-inventory]   elec-003=12  home-004=11  clth-001=9
    [fluxcart-inventory]   ── Top Categories ────────────────────────────────
    [fluxcart-inventory]   electronics=41  home=38  clothing=32
    [fluxcart-inventory]   ── Warehouse Activity ────────────────────────────
    [fluxcart-inventory]   wh-mumbai=38  wh-delhi=31  wh-bangalore=28
    [fluxcart-inventory] ═════════════════════════════════════════════════════

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
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumers.base_consumer import BaseConsumer
# Inherits: poll loop, offset commits, dead letter routing, shutdown handling

from config import (
    TOPIC_ORDERS,        # "fluxcart.orders"
    GROUP_INVENTORY,     # "fluxcart-inventory"
    consumer_config,     # factory function that builds consumer settings dict
)

from collections import defaultdict


# =============================================================================
# INVENTORY CONSUMER
# =============================================================================

class InventoryConsumer(BaseConsumer):
    """
    Reads order events from Kafka.
    Tracks stock movement and warehouse activity in real time.

    Inherits all infrastructure from BaseConsumer.
    Implements: topics(), process(), emit_report()
    """

    def __init__(self):
        """
        Set up the inventory consumer.
        Initialize window counters, then connect to Kafka.
        """

        # Initialize window counters before super().__init__()
        # so all attributes exist before the poll loop can call process()
        self._reset_window()

        # Connect to Kafka and subscribe to topics
        super().__init__(
            group_id     = GROUP_INVENTORY,
            consumer_cfg = consumer_config(GROUP_INVENTORY),
            # offset_reset defaults to "earliest" — inventory reads full history
            # so it has a complete picture of all orders since the pipeline started
        )

    # =========================================================================
    # WINDOW STATE
    # =========================================================================

    def _reset_window(self):
        """
        Reset all window counters to zero.
        Called at startup and after every emit_report().
        """

        # ── Order counts ──────────────────────────────────────────────────
        self.order_count = 0
        # Total order events received this window — all statuses combined

        self.status_counts = defaultdict(int)
        # Count per status: {"placed": 58, "confirmed": 30, "shipped": 19}
        # Gives a snapshot of the order lifecycle distribution right now

        # ── Stock movement ────────────────────────────────────────────────
        self.units_ordered = 0
        # Total units ordered across all "placed" events this window
        # This is the demand signal — how much stock is being requested

        self.units_delivered = 0
        # Total units successfully delivered this window
        # This is fulfillment output — how much stock actually reached customers

        self.units_cancelled = 0
        # Total units from cancelled orders this window
        # High cancellations = possible fraud, bad product listings, or stock issues

        # ── Product tracking ──────────────────────────────────────────────
        self.product_order_counts = defaultdict(int)
        # How many order events per product: {"elec-003": 12, "home-004": 11}
        # Used to find the fastest moving products

        self.product_units = defaultdict(int)
        # How many units per product: {"elec-003": 15, "home-004": 14}
        # Counts quantity, not just events — an order for 3 units counts as 3

        # ── Category tracking ─────────────────────────────────────────────
        self.category_counts = defaultdict(int)
        # How many order events per category: {"electronics": 41, "home": 38}

        self.category_units = defaultdict(int)
        # How many units per category — quantity weighted

        # ── Warehouse tracking ────────────────────────────────────────────
        self.warehouse_counts = defaultdict(int)
        # How many orders each warehouse is processing: {"wh-mumbai": 38}
        # Used to spot overloaded or underutilised warehouses

    # =========================================================================
    # topics()
    # =========================================================================

    def topics(self) -> list:
        """
        Subscribe to orders topic only.
        Inventory does not need behavior or payment events.
        """
        return [TOPIC_ORDERS]

    # =========================================================================
    # process()
    # =========================================================================

    def process(self, event: dict, topic: str):
        """
        Process one order event.

        Args:
            event: OrderEvent as dict
                   keys: event_id, timestamp, order_id, user_id, product_id,
                         category, quantity, unit_price, order_total,
                         status, warehouse_id
            topic: always TOPIC_ORDERS for this consumer
        """

        # ── Count every order event ───────────────────────────────────────
        self.order_count += 1
        # Increment total regardless of status

        status   = event["status"]
        quantity = event["quantity"]      # how many units in this order
        product  = event["product_id"]
        category = event["category"]
        warehouse = event["warehouse_id"]

        self.status_counts[status] += 1
        # Track lifecycle distribution across all statuses

        # ── Stock movement by status ──────────────────────────────────────

        if status == "placed":
            self.units_ordered += quantity
            # New demand — stock needs to be reserved for this order
            # quantity is the number of units, not always 1
            # e.g. an order for 3 electronics = units_ordered += 3

        elif status == "delivered":
            self.units_delivered += quantity
            # Fulfilled demand — stock successfully reached the customer

        elif status == "cancelled":
            self.units_cancelled += quantity
            # Cancelled demand — reserved stock is released back

        # ── Product and category tracking ─────────────────────────────────
        # Track for ALL statuses — gives full picture of product activity
        # not just new orders

        self.product_order_counts[product] += 1
        # One more order event for this product (regardless of quantity)

        self.product_units[product] += quantity
        # Add the actual units to the product's unit count
        # An order of 3 units adds 3, not 1

        self.category_counts[category] += 1
        # One more order event for this category

        self.category_units[category] += quantity
        # Add actual units to the category unit count

        # ── Warehouse tracking ────────────────────────────────────────────
        self.warehouse_counts[warehouse] += 1
        # Track which warehouse handled this order event
        # Shows workload distribution across warehouses

    # =========================================================================
    # emit_report()
    # =========================================================================

    def emit_report(self):
        """
        Print the inventory window summary and reset counters.
        Called every CONSUMER_WINDOW_SECONDS (10) seconds.
        """

        g = self.group_id

        # ── Header ────────────────────────────────────────────────────────
        print(f"\n[{g}] {chr(9553)*2} INVENTORY WINDOW (10s) {'=' * 40}")

        # ── Order Count ───────────────────────────────────────────────────
        print(f"[{g}]   Orders received    : {self.order_count}")

        # ── Status Breakdown ──────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        status_str = "  ".join(
            f"{status}={count}"
            for status, count in sorted(
                self.status_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   By status          : {status_str or 'none'}")

        # ── Stock Movement ────────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        print(f"[{g}]   Units ordered      : {self.units_ordered}")
        # units_ordered = total new demand this window

        print(f"[{g}]   Units delivered    : {self.units_delivered}")
        # units_delivered = total fulfilled this window

        print(f"[{g}]   Units cancelled    : {self.units_cancelled}")
        # units_cancelled = demand that did not convert

        # Fulfillment rate — what percentage of ordered units were delivered?
        # Only calculate if we have ordered units to avoid division by zero
        if self.units_ordered > 0:
            fulfillment_rate = (self.units_delivered / self.units_ordered) * 100
            print(f"[{g}]   Fulfillment rate   : {fulfillment_rate:.1f}%")
            # fulfillment_rate = delivered / ordered × 100
            # 100% = every unit ordered this window was also delivered
            # In practice this will be low because delivery happens later

        # ── Top Products ──────────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")

        # Sort products by units ordered (quantity weighted), show top 5
        top_products = sorted(
            self.product_units.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        product_str = "  ".join(f"{p}={u}units" for p, u in top_products)
        print(f"[{g}]   Top products       : {product_str or 'none'}")

        # ── Top Categories ────────────────────────────────────────────────
        top_categories = sorted(
            self.category_units.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        category_str = "  ".join(f"{cat}={u}units" for cat, u in top_categories)
        print(f"[{g}]   Top categories     : {category_str or 'none'}")

        # ── Warehouse Activity ────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        warehouse_str = "  ".join(
            f"{wh}={count}"
            for wh, count in sorted(
                self.warehouse_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   Warehouse activity : {warehouse_str or 'none'}")

        # ── Footer ────────────────────────────────────────────────────────
        print(f"[{g}] {'=' * 62}\n")

        # Reset all counters for the next window
        self._reset_window()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Create and run the inventory consumer."""
    consumer = InventoryConsumer()
    consumer.run()
    # run() is defined in BaseConsumer.
    # Loops forever: poll → process → commit → repeat
    # Until Ctrl+C, then commits final batch and closes cleanly.


if __name__ == "__main__":
    main()