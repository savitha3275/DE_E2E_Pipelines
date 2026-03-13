"""
dashboard.py — FluxCart Live Terminal Dashboard
================================================

PURPOSE:
    Displays a unified live view of the entire pipeline in one terminal.
    Instead of watching 4 separate terminals, this single screen shows
    analytics, fraud, and inventory side by side, refreshing every 10 seconds.

    This is a READ-ONLY view. It does not consume from Kafka directly.
    It runs alongside run_pipeline.py and reads shared state from the
    consumer instances.

    BUT — if you want to run it standalone (without run_pipeline.py),
    it creates its own consumer instances and runs them internally.

HOW TO RUN (two ways):

    Way 1 — Standalone (creates its own consumers):
        Terminal 1: python3 producer.py
        Terminal 2: python3 monitoring/dashboard.py

    Way 2 — After run_pipeline.py is already running:
        This file can also be imported by run_pipeline.py
        to show the unified view instead of 4 separate outputs.



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

import threading   # for running consumers in background threads
import time        # for the refresh interval
import os          # for os.system("clear") to clear the terminal

from consumers.analytics import AnalyticsConsumer
from consumers.fraud      import FraudConsumer
from consumers.inventory  import InventoryConsumer


# =============================================================================
# DASHBOARD WIDTH
# =============================================================================

WIDTH = 68
# Total width of the dashboard in characters.
# Every line is padded or truncated to fit this width.
# Change this if your terminal is wider or narrower.


# =============================================================================
# DASHBOARD RENDERER
# =============================================================================

class Dashboard:
    """
    Renders a live terminal dashboard showing all consumer states.

    Creates three consumers, runs them in background threads,
    and reads their state every REFRESH_SECONDS to render the display.
    """

    REFRESH_SECONDS = 10
    # How often to redraw the dashboard.
    # Matches CONSUMER_WINDOW_SECONDS so the dashboard updates
    # at the same rate as the consumers emit their window reports.

    def __init__(self):
        """
        Create the three consumer instances and set up threads.
        """

        print("Initializing FluxCart Dashboard...")
        print("Starting consumers in background threads...")
        print()

        # Create consumer instances
        # These are the same classes used in run_pipeline.py
        self.analytics = AnalyticsConsumer()
        self.fraud      = FraudConsumer()
        self.inventory  = InventoryConsumer()

        # List of (consumer, name) pairs for easy iteration
        self.consumers = [
            (self.analytics, "analytics"),
            (self.fraud,     "fraud"),
            (self.inventory, "inventory"),
        ]

        self.threads = []
        # Stores the thread objects so we can join them on shutdown

        self.running = True
        # Flag that controls the dashboard render loop
        # Set to False on Ctrl+C to stop the render loop

    def _start_consumer_threads(self):
        """
        Start each consumer in its own background thread.
        The consumers run independently and update their internal state.
        The dashboard reads that state to render the display.
        """
        for consumer, name in self.consumers:
            thread = threading.Thread(
                target = consumer.run,
                # consumer.run() is the poll loop from BaseConsumer
                # It runs forever until consumer.stop() is called
                daemon = True,
                name   = f"dashboard-{name}",
            )
            thread.start()
            self.threads.append(thread)
            print(f"  ✅  {name} consumer started")

        print()
        print("All consumers running. Dashboard starting in 3 seconds...")
        time.sleep(3)
        # Give consumers time to connect and get partition assignments
        # before we start rendering the dashboard

    # =========================================================================
    # RENDERING HELPERS
    # =========================================================================
    # These small helper methods build formatted lines for the dashboard.
    # Each one returns a string padded to exactly WIDTH characters.

    def _top_line(self) -> str:
        """Top border of the dashboard box."""
        return "╔" + "═" * (WIDTH - 2) + "╗"

    def _bottom_line(self) -> str:
        """Bottom border of the dashboard box."""
        return "╚" + "═" * (WIDTH - 2) + "╝"

    def _divider(self) -> str:
        """Horizontal divider between sections."""
        return "╠" + "═" * (WIDTH - 2) + "╣"

    def _row(self, text: str = "") -> str:
        """
        One content row. Pads text to fit within the box borders.

        Args:
            text: the content to display on this row
        Returns:
            formatted row string exactly WIDTH characters wide
        """
        # Truncate if text is too long, pad with spaces if too short
        inner = text[:WIDTH - 4]           # leave room for ║  and  ║
        padded = inner.ljust(WIDTH - 4)    # pad with spaces to fill the row
        return f"║  {padded}  ║"

    def _title_row(self, title: str) -> str:
        """Centered title row."""
        centered = title.center(WIDTH - 4)
        return f"║  {centered}  ║"

    def _blank(self) -> str:
        """Empty row — used for spacing."""
        return self._row("")

    # =========================================================================
    # SECTION RENDERERS
    # =========================================================================

    def _render_analytics(self) -> list:
        """
        Build the analytics section rows.
        Reads current state from self.analytics counters.

        Returns:
            list of formatted row strings
        """
        a = self.analytics
        # a is a shorthand for self.analytics
        # We read its window counters directly — action_counts, product_counts etc.

        rows = []
        rows.append(self._row("ANALYTICS"))
        rows.append(self._row(f"  Behavior events   : {a.behavior_count}"))

        # Top action
        if a.action_counts:
            top_action = max(a.action_counts, key=a.action_counts.get)
            # max() with key finds the action with the highest count
            rows.append(self._row(f"  Top action        : {top_action}={a.action_counts[top_action]}"))

        # Top product
        if a.product_counts:
            top_product = max(a.product_counts, key=a.product_counts.get)
            rows.append(self._row(f"  Top product       : {top_product}={a.product_counts[top_product]}"))

        # Top category
        if a.category_counts:
            top_cat = max(a.category_counts, key=a.category_counts.get)
            rows.append(self._row(f"  Top category      : {top_cat}={a.category_counts[top_cat]}"))

        # Orders
        rows.append(self._row(f"  Order events      : {a.order_count}"))

        if a.order_totals:
            avg = sum(a.order_totals) / len(a.order_totals)
            total = sum(a.order_totals)
            rows.append(self._row(f"  Avg order value   : Rs.{avg:,.0f}"))
            rows.append(self._row(f"  Total revenue     : Rs.{total:,.0f}"))

        # Overall health
        rows.append(self._row(f"  Total processed   : {a.total_processed}"))

        return rows

    def _render_fraud(self) -> list:
        """
        Build the fraud section rows.
        Reads current state from self.fraud counters.
        """
        f = self.fraud

        rows = []
        rows.append(self._row("FRAUD DETECTION"))
        rows.append(self._row(f"  Payments processed : {f.payment_count}"))
        rows.append(self._row(f"  Failed payments    : {f.failed_payment_count}"))
        rows.append(self._row(f"  Alerts raised      : {len(f.alerts_this_window)}"))

        if f.alerts_this_window:
            # Show up to 3 most recent alerts
            for alert in f.alerts_this_window[-3:]:
                line = (f"  {alert['severity']:<6} "
                        f"{alert['rule'][:25]:<25}  "
                        f"Rs.{alert['amount']:,.0f}")
                rows.append(self._row(line))
        else:
            rows.append(self._row("  No fraud alerts this window — all clear ✅"))

        rows.append(self._row(f"  Total processed   : {f.total_processed}"))
        rows.append(self._row(f"  Order events      : {f.order_count}"))
        rows.append(self._row(f"  Cancelled orders  : {f.cancelled_order_count}"))

        return rows

    def _render_inventory(self) -> list:
        """
        Build the inventory section rows.
        Reads current state from self.inventory counters.
        """
        inv = self.inventory

        rows = []
        rows.append(self._row("INVENTORY"))
        rows.append(self._row(f"  Orders received   : {inv.order_count}"))
        rows.append(self._row(f"  Units ordered     : {inv.units_ordered}"))
        rows.append(self._row(f"  Units delivered   : {inv.units_delivered}"))
        rows.append(self._row(f"  Units cancelled   : {inv.units_cancelled}"))

        if inv.units_ordered > 0:
            rate = (inv.units_delivered / inv.units_ordered) * 100
            rows.append(self._row(f"  Fulfillment rate  : {rate:.1f}%"))

        # Top warehouse
        if inv.warehouse_counts:
            top_wh = max(inv.warehouse_counts, key=inv.warehouse_counts.get)
            rows.append(self._row(f"  Busiest warehouse : {top_wh}={inv.warehouse_counts[top_wh]}"))

        # Top product by units
        if inv.product_units:
            top_prod = max(inv.product_units, key=inv.product_units.get)
            rows.append(self._row(f"  Top product       : {top_prod}={inv.product_units[top_prod]} units"))

        rows.append(self._row(f"  Total processed   : {inv.total_processed}"))

        return rows

    def _render_health(self) -> list:
        """
        Build the pipeline health section.
        Shows processed counts and error counts for all consumers.
        """
        rows = []
        rows.append(self._row("PIPELINE HEALTH"))

        for consumer, name in self.consumers:
            status = "✅" if consumer.total_errors == 0 else "⚠️ "
            rows.append(self._row(
                f"  {name:<12}  {status}  "
                f"processed={consumer.total_processed}  "
                f"errors={consumer.total_errors}"
            ))

        return rows

    # =========================================================================
    # FULL RENDER
    # =========================================================================

    def _render(self):
        """
        Clear the terminal and draw the complete dashboard.

        Called every REFRESH_SECONDS by the render loop.
        """

        # Clear the terminal — gives the "live refresh" effect
        os.system("clear")
        # os.system("clear") sends the clear command to the terminal
        # On Windows this would be os.system("cls")

        lines = []

        # ── Build all lines ───────────────────────────────────────────────
        lines.append(self._top_line())
        lines.append(self._title_row("FLUXCART LIVE PIPELINE DASHBOARD"))
        lines.append(self._row(f"  Refreshing every {self.REFRESH_SECONDS}s  |  Press Ctrl+C to stop"))
        lines.append(self._divider())

        # Analytics section
        for row in self._render_analytics():
            lines.append(row)
        lines.append(self._divider())

        # Fraud section
        for row in self._render_fraud():
            lines.append(row)
        lines.append(self._divider())

        # Inventory section
        for row in self._render_inventory():
            lines.append(row)
        lines.append(self._divider())

        # Health section
        for row in self._render_health():
            lines.append(row)
        lines.append(self._bottom_line())

        # ── Print all lines ───────────────────────────────────────────────
        print("\n".join(lines))

    # =========================================================================
    # RENDER LOOP
    # =========================================================================

    def run(self):
        """
        Start consumer threads and run the render loop.

        Clears and redraws the dashboard every REFRESH_SECONDS.
        Runs until Ctrl+C is pressed.
        """

        # Start all consumer threads in the background
        self._start_consumer_threads()

        try:
            while self.running:
                self._render()
                # Draw the current state of all consumers

                time.sleep(self.REFRESH_SECONDS)
                # Wait before redrawing
                # During this sleep, consumers are still processing
                # events and updating their counters in the background

        except KeyboardInterrupt:
            print("\n\nCtrl+C received — stopping dashboard...")
            self._shutdown()

    def _shutdown(self):
        """Stop all consumers and wait for threads to finish."""

        for consumer, name in self.consumers:
            consumer.stop()
            # Sets each consumer's shutdown_event flag
            # The poll loop exits on the next iteration

        for thread in self.threads:
            thread.join(timeout=10)
            # Wait for each thread to finish cleanly

        print("Dashboard stopped.")


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Create and run the dashboard."""
    dashboard = Dashboard()
    dashboard.run()


if __name__ == "__main__":
    main()