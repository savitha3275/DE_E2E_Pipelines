"""
fraud.py — FluxCart Fraud Detection Consumer
=============================================

Run this and then open a new terminal and there run producer.py too
before that make sure you execute : docker compose up -d and python3 setup_topics.py
PURPOSE:
    Reads payment events and order events from Kafka.
    Applies fraud detection rules to every payment in real time.
    When fraud is detected, raises an alert to fluxcart.fraud.alerts.

    This answers questions like:
        - Is this a large payment from a brand new account?
        - Are payments failing repeatedly for the same order?
        - Are there unusually large payments this window?

FRAUD RULES (3 rules, applied to every payment event):

    RULE 1 — Large payment from new account
        If payment amount > Rs.50,000 AND account age < 7 days
        → HIGH RISK: new accounts making large purchases is a major fraud signal
        → Real world: stolen card used immediately on expensive items

    RULE 2 — Very large payment (any account)
        If payment amount > Rs.200,000
        → MEDIUM RISK: any payment above Rs.2 lakh needs review
        → Real world: unusually large transaction for the platform

    RULE 3 — Failed payment
        If payment status == "failed"
        → LOW RISK: single failure is normal, but tracked for pattern analysis
        → Real world: multiple failures = card testing attack

WHAT IT READS:
    fluxcart.payments  — every payment event (initiated, success, failed, refunded)
    fluxcart.orders    — every order event (for context and pattern matching)

WHAT IT WRITES:
    fluxcart.fraud.alerts — one alert per fraud detection
    (The base class already has a dead letter producer for broken messages.
     The fraud consumer adds a SECOND producer specifically for fraud alerts.)

CONSUMER GROUP:
    fluxcart-fraud

HOW TO RUN:
    python3 consumers/fraud.py

EXPECTED OUTPUT (every 10 seconds):
    [fluxcart-fraud] ══ FRAUD WINDOW (10s) ══════════════════════════════════
    [fluxcart-fraud]   Payments processed : 58
    [fluxcart-fraud]   By status          : success=32  initiated=17  failed=7  refunded=2
    [fluxcart-fraud]   Failed payments    : 7
    [fluxcart-fraud]   ── Fraud Alerts ────────────────────────────────────────
    [fluxcart-fraud]   Alerts raised      : 3
    [fluxcart-fraud]   HIGH  new_account_large_payment  user=user-0023  Rs.87,400
    [fluxcart-fraud]   MEDIUM large_payment             user=user-0041  Rs.245,000
    [fluxcart-fraud]   LOW   failed_payment             user=user-0017  Rs.12,400
    [fluxcart-fraud]   ── Orders ──────────────────────────────────────────────
    [fluxcart-fraud]   Order events       : 113
    [fluxcart-fraud]   Cancelled orders   : 4
    [fluxcart-fraud] ═══════════════════════════════════════════════════════════

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
    TOPIC_PAYMENTS,       # "fluxcart.payments"
    TOPIC_ORDERS,         # "fluxcart.orders"
    TOPIC_FRAUD_ALERTS,   # "fluxcart.fraud.alerts"
    GROUP_FRAUD,          # "fluxcart-fraud"
    consumer_config,      # builds consumer config dict
    PRODUCER_CONFIG,      # producer settings — reused for alert producer
)

from confluent_kafka import Producer
# We need a Producer to write fraud alerts to fluxcart.fraud.alerts.
# The base class already has a dead letter producer.
# This is a second, separate producer just for fraud alerts.

from collections import defaultdict
import json     # for serializing fraud alert events to bytes
import time     # for timestamps on fraud alerts


# =============================================================================
# FRAUD THRESHOLDS — the rules as named constants
# =============================================================================
# Named constants instead of magic numbers scattered through the code.
# If the risk team changes the threshold, we change it in ONE place.

LARGE_PAYMENT_THRESHOLD     = 50_000   # Rs.50,000 — large payment threshold
VERY_LARGE_PAYMENT_THRESHOLD = 200_000  # Rs.200,000 — very large payment threshold
NEW_ACCOUNT_AGE_DAYS        = 7        # accounts younger than 7 days are "new"


# =============================================================================
# FRAUD CONSUMER
# =============================================================================

class FraudConsumer(BaseConsumer):
    """
    Reads payment and order events from Kafka.
    Applies fraud detection rules in real time.
    Publishes fraud alerts to fluxcart.fraud.alerts.

    Inherits all infrastructure from BaseConsumer.
    Implements: topics(), process(), emit_report()
    """

    def __init__(self):
        """
        Set up the fraud consumer.

        Two extra steps compared to AnalyticsConsumer:
            1. Initialize window counters
            2. Create a fraud alert producer (separate from dead letter producer)
            3. Call super().__init__() to connect Kafka and subscribe
        """

        # Initialize window counters first — before super().__init__()
        # because super() may trigger emit_report() during setup
        self._reset_window()

        # ── Fraud Alert Producer ──────────────────────────────────────────
        # A dedicated producer for writing fraud alerts.
        # Separate from the dead letter producer in BaseConsumer.
        # Dead letter = broken messages we cannot process
        # Fraud alerts = valid messages that triggered a fraud rule
        # These are different concepts and deserve separate producers.
        alert_config = {
            "bootstrap.servers": PRODUCER_CONFIG["bootstrap.servers"],
            "client.id":         f"{GROUP_FRAUD}-alert-producer",
            "acks":              "all",   # fraud alerts must not be lost — use all acks
        }
        self.alert_producer = Producer(alert_config)
        # acks="all" for fraud alerts — unlike dead letter (acks="1"),
        # fraud alerts are business-critical. We must not lose them.

        # ── Connect to Kafka and Subscribe ────────────────────────────────
        super().__init__(
            group_id     = GROUP_FRAUD,
            consumer_cfg = consumer_config(GROUP_FRAUD, offset_reset="latest"),
        )
        # offset_reset="latest" — fraud detection is real-time only.
        # We do not want to evaluate old historical payments as fraud.
        # "latest" means: start reading from NOW, skip everything before startup.
        # Compare with analytics which uses "earliest" to read all history.

    # =========================================================================
    # WINDOW STATE
    # =========================================================================

    def _reset_window(self):
        """
        Reset all window counters to zero.
        Called at startup and after every emit_report().
        """

        # ── Payment counters ──────────────────────────────────────────────
        self.payment_count = 0
        # Total payment events received this window

        self.payment_status_counts = defaultdict(int)
        # Count per status: {"success": 32, "initiated": 17, "failed": 7}

        self.failed_payment_count = 0
        # How many payments failed this window — tracked separately
        # because failed payments are an important fraud signal

        # ── Fraud alert tracking ──────────────────────────────────────────
        self.alerts_this_window = []
        # List of fraud alert dictionaries raised this window.
        # Each alert is a dict with: rule, severity, user_id, amount
        # We store them all so emit_report() can print each one.

        # ── Order counters ────────────────────────────────────────────────
        self.order_count = 0
        # Total order events received this window

        self.cancelled_order_count = 0
        # How many orders were cancelled — another fraud/churn signal

    # =========================================================================
    # topics()
    # =========================================================================

    def topics(self) -> list:
        """Which Kafka topics to subscribe to."""
        return [TOPIC_PAYMENTS, TOPIC_ORDERS]
        # Payments — the primary source for fraud detection
        # Orders — for context: large orders from new accounts, cancellations

    # =========================================================================
    # process()
    # =========================================================================

    def process(self, event: dict, topic: str):
        """
        Route each event to the correct handler based on its source topic.

        Args:
            event: deserialized event dictionary
            topic: which topic this event came from
        """
        if topic == TOPIC_PAYMENTS:
            self._process_payment(event)
        elif topic == TOPIC_ORDERS:
            self._process_order(event)

    def _process_payment(self, event: dict):
        """
        Apply all fraud rules to one payment event.

        Called for every payment event received.
        Updates counters and raises alerts if rules are triggered.

        Args:
            event: PaymentEvent as dict
                   keys: event_id, timestamp, payment_id, order_id, user_id,
                         amount, currency, payment_method, status, account_age_days
        """

        self.payment_count += 1
        # Count this payment regardless of outcome

        status = event["status"]
        self.payment_status_counts[status] += 1
        # Track the lifecycle distribution: success, initiated, failed, refunded

        # ── Track failed payments ─────────────────────────────────────────
        if status == "failed":
            self.failed_payment_count += 1
            # Failed payments are tracked separately for pattern analysis.
            # One failure = normal. Five failures in 10 seconds = card testing.

        # ── Apply fraud rules ─────────────────────────────────────────────
        # Only evaluate fraud rules on initiated or success payments.
        # Refunded and failed payments are already in a terminal state.
        if status in ("initiated", "success"):
            self._apply_fraud_rules(event)

    def _apply_fraud_rules(self, event: dict):
        """
        Apply the three fraud detection rules to one payment.

        Rules are evaluated independently — a single payment can trigger
        multiple rules simultaneously (e.g. very large + new account).

        Args:
            event: PaymentEvent dict (already confirmed status is active)
        """

        amount          = event["amount"]           # payment amount in INR
        account_age     = event["account_age_days"] # how old is this account
        user_id         = event["user_id"]
        payment_id      = event["payment_id"]
        order_id        = event["order_id"]

        # ── RULE 1: Large payment from new account ────────────────────────
        # A payment over Rs.50,000 from an account less than 7 days old.
        # This is the highest risk pattern — stolen cards are often used
        # immediately on expensive purchases before the owner notices.
        if amount > LARGE_PAYMENT_THRESHOLD and account_age < NEW_ACCOUNT_AGE_DAYS:
            self._raise_alert(
                rule       = "new_account_large_payment",
                severity   = "HIGH",
                user_id    = user_id,
                payment_id = payment_id,
                order_id   = order_id,
                amount     = amount,
                detail     = f"account age={account_age} days, amount=Rs.{amount:,.0f}",
            )

        # ── RULE 2: Very large payment ────────────────────────────────────
        # Any payment over Rs.200,000 regardless of account age.
        # Unusual for this platform — warrants human review.
        elif amount > VERY_LARGE_PAYMENT_THRESHOLD:
            self._raise_alert(
                rule       = "very_large_payment",
                severity   = "MEDIUM",
                user_id    = user_id,
                payment_id = payment_id,
                order_id   = order_id,
                amount     = amount,
                detail     = f"amount=Rs.{amount:,.0f} exceeds threshold",
            )

        # ── RULE 3: Failed payment ────────────────────────────────────────
        # Track individual payment failures.
        # Single failure is low risk. Pattern of failures = card testing.
        # We raise a LOW alert for every failure so analysts can spot patterns.
        if event["status"] == "failed":
            self._raise_alert(
                rule       = "failed_payment",
                severity   = "LOW",
                user_id    = user_id,
                payment_id = payment_id,
                order_id   = order_id,
                amount     = amount,
                detail     = f"payment failed via {event['payment_method']}",
            )

    def _raise_alert(
        self,
        rule: str,
        severity: str,
        user_id: str,
        payment_id: str,
        order_id: str,
        amount: float,
        detail: str,
    ):
        """
        Build a fraud alert, store it for the window report,
        and publish it to fluxcart.fraud.alerts.

        Args:
            rule:       which rule was triggered (e.g. "new_account_large_payment")
            severity:   "HIGH", "MEDIUM", or "LOW"
            user_id:    who triggered the alert
            payment_id: the payment that triggered it
            order_id:   the associated order
            amount:     payment amount in INR
            detail:     human-readable explanation of why this alert fired
        """

        # Build the alert event — this is what gets written to Kafka
        alert = {
            "alert_id":   f"alert-{payment_id}",   # unique ID for this alert
            "raised_at":  time.time(),              # Unix timestamp
            "rule":       rule,                     # which rule fired
            "severity":   severity,                 # HIGH / MEDIUM / LOW
            "user_id":    user_id,
            "payment_id": payment_id,
            "order_id":   order_id,
            "amount":     amount,
            "detail":     detail,
        }

        # Store the alert for the window report
        # We print all alerts at the end of each window
        self.alerts_this_window.append(alert)

        # Publish the alert to fluxcart.fraud.alerts
        # This is where a real system would trigger an email, SMS,
        # block the transaction, or notify a fraud analyst dashboard.
        try:
            self.alert_producer.produce(
                topic = TOPIC_FRAUD_ALERTS,
                key   = user_id.encode("utf-8"),   # key by user — all alerts for same user → same partition
                value = json.dumps(alert).encode("utf-8"),
            )
            self.alert_producer.poll(0)
            # poll(0) triggers delivery callbacks without blocking —
            # same pattern as the main producer in producer.py

        except Exception as e:
            # If alert publishing fails, log it but do not crash the consumer.
            # We still have the alert in alerts_this_window for reporting.
            print(f"[{self.group_id}] Alert publish failed: {e}")

    def _process_order(self, event: dict):
        """
        Track order events for fraud context.

        Args:
            event: OrderEvent as dict
        """

        self.order_count += 1

        if event["status"] == "cancelled":
            self.cancelled_order_count += 1
            # Track cancellations — high cancellation rate can signal
            # fraudulent orders being placed and then abandoned.

    # =========================================================================
    # emit_report()
    # =========================================================================

    def emit_report(self):
        """
        Print the fraud window summary and reset counters.
        Called every CONSUMER_WINDOW_SECONDS (10) seconds.
        """

        # Flush pending alerts before reporting
        # Ensures all alerts are delivered before we print the summary
        self.alert_producer.flush()

        g = self.group_id

        # ── Header ────────────────────────────────────────────────────────
        print(f"\n[{g}] {chr(9553)*2} FRAUD WINDOW (10s) {'=' * 44}")

        # ── Payment Section ───────────────────────────────────────────────
        print(f"[{g}]   Payments processed : {self.payment_count}")

        status_str = "  ".join(
            f"{status}={count}"
            for status, count in sorted(
                self.payment_status_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   By status          : {status_str or 'none'}")
        print(f"[{g}]   Failed payments    : {self.failed_payment_count}")

        # ── Fraud Alerts Section ──────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        print(f"[{g}]   Alerts raised      : {len(self.alerts_this_window)}")

        if self.alerts_this_window:
            for alert in self.alerts_this_window:
                # Print one line per alert: severity, rule, user, amount
                print(
                    f"[{g}]   {alert['severity']:<6}  "
                    f"{alert['rule']:<30}  "
                    f"user={alert['user_id']}  "
                    f"Rs.{alert['amount']:,.0f}"
                )
        else:
            print(f"[{g}]   No fraud alerts this window — all clear")

        # ── Orders Section ────────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        print(f"[{g}]   Order events       : {self.order_count}")
        print(f"[{g}]   Cancelled orders   : {self.cancelled_order_count}")

        # ── Footer ────────────────────────────────────────────────────────
        print(f"[{g}] {'=' * 62}\n")

        # Reset counters for next window
        self._reset_window()


# =============================================================================
# ENTRY POINT
# =============================================================================

def main():
    """Create and run the fraud consumer."""
    consumer = FraudConsumer()
    consumer.run()


if __name__ == "__main__":
    main()