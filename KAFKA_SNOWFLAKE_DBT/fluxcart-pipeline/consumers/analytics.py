'''
analytics.py - Fluxcart Analytics Consumer

reads user behvaior events and order events from kafka
aggregates them into live window report every 10 seconds

what it reads : fluxcart.user.behavior and fluxcart.orders
CONSUMER GROUP
fluxcart-analytics

'''

import sys
import os
# add the project root to python's search path
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumers.base_consumer import BaseConsumer

from config import (
    TOPIC_USER_BEHAVIOR,
    TOPIC_ORDERS,
    GROUP_ANALYTICS,
    consumer_config,
)

from collections import defaultdict

# analytics consumer

class AnalyticsConsumer(BaseConsumer):
    '''
    reads the behavior and order events from kafka
    
    inherits all the infra from BaseConsumer
    '''
    
    def __init__(self):
        self._reset_window()
        super().__init__(
            group_id=GROUP_ANALYTICS,
            consumer_cfg=consumer_config(GROUP_ANALYTICS))
        
        
    # window. state - counters that reset every 10 seconds
    def _reset_window(self):
        '''
        reset all window counters to zero
        
        '''
        # behvaiour event counters
        self.behavior_count=0
        # total behaviour events received
        # incremented once per behaviour event 
        self.action_counts = defaultdict(int)
        # {"view":162,"add_to_cart": 400}
        self.product_counts = defaultdict(int)
        self.category_counts = defaultdict(int)
        self.device_counts = defaultdict(int)
        self.country_counts = defaultdict(int)
        # order event counters
        self.order_count = 0
        self.order_status_counts = defaultdict(int)
        self.order_totals= []
        
    # topics() which topics to read from
    def topics(self) -> list:
        return [TOPIC_USER_BEHAVIOR,TOPIC_ORDERS]
    
    # process() - what to do with each event
    def process(self,event:dict,topic:str):
        if topic == TOPIC_USER_BEHAVIOR:
            self._process_behavior(event)
            # user did something - view ,search, add to cart
        elif topic == TOPIC_ORDERS:
            self._process_order(event)
            
    # process behavior
    
    def _process_behavior(self,event:dict):
        self.behavior_count += 1
        self.action_counts[event["action"]] += 1
        self.product_counts[event["product_id"]] += 1
        self.category_counts[event["category"]] += 1 
        self.device_counts[event["device"]] += 1
        self.country_counts[event["country"]] += 1
        
    # process order
    
    def _process_order(self,event:dict):
        self.order_count += 1 
        self.order_status_counts[event["status"]] += 1
        self.order_totals.append(event["order_total"])
        
    
    def emit_report(self):
        """
        Print the aggregated window summary and reset all counters.
 
        Called by the base class every CONSUMER_WINDOW_SECONDS (10) seconds.
        After printing, resets all counters so the next window starts fresh.
        """
 
        g = self.group_id   # shorter alias for cleaner print statements
 
        # ── Header ────────────────────────────────────────────────────────
        print(f"\n[{g}] {chr(9553)*2} ANALYTICS WINDOW (10s) {'=' * 40}")
 
        # ── Behavior Section ──────────────────────────────────────────────
        print(f"[{g}]   Behavior events  : {self.behavior_count}")
 
        # Action breakdown — sorted by count, highest first
        # sorted(dict.items()) sorts the key-value pairs
        # key=lambda x: x[1] sorts by the value (the count)
        # reverse=True puts the highest count first
        action_str = "  ".join(
            f"{action}={count}"
            for action, count in sorted(
                self.action_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   Actions          : {action_str or 'none'}")
 
        # Top 5 most interacted products
        # sorted() returns all products by count, [:5] takes only top 5
        top_products = sorted(
            self.product_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        product_str = "  ".join(f"{p}={c}" for p, c in top_products)
        print(f"[{g}]   Top products     : {product_str or 'none'}")
 
        # Top 3 categories
        top_categories = sorted(
            self.category_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        category_str = "  ".join(f"{cat}={cnt}" for cat, cnt in top_categories)
        print(f"[{g}]   Top categories   : {category_str or 'none'}")
 
        # Device breakdown
        device_str = "  ".join(
            f"{d}={c}"
            for d, c in sorted(
                self.device_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   Devices          : {device_str or 'none'}")
 
        # Top 3 countries
        top_countries = sorted(
            self.country_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3]
        country_str = "  ".join(f"{c}={n}" for c, n in top_countries)
        print(f"[{g}]   Top countries    : {country_str or 'none'}")
 
        # ── Orders Section ────────────────────────────────────────────────
        print(f"[{g}]   {'-'*55}")
        print(f"[{g}]   Order events     : {self.order_count}")
 
        # Order status breakdown
        status_str = "  ".join(
            f"{status}={count}"
            for status, count in sorted(
                self.order_status_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )
        )
        print(f"[{g}]   Statuses         : {status_str or 'none'}")
 
        # Average order value and total revenue
        if self.order_totals:
            avg_order = sum(self.order_totals) / len(self.order_totals)
            # sum() adds all order_total values in the list
            # len() counts how many orders there were
            # dividing gives us the true average
 
            total_rev = sum(self.order_totals)
            # Total revenue = sum of all order values this window
 
            print(f"[{g}]   Avg order value  : Rs.{avg_order:,.2f}")
            # :,.2f = format with comma separators and 2 decimal places
            # 18432.5 becomes "18,432.50"
 
            print(f"[{g}]   Total revenue    : Rs.{total_rev:,.2f}")
        else:
            print(f"[{g}]   No order data in this window")
 
        # ── Footer ────────────────────────────────────────────────────────
        print(f"[{g}] {'=' * 62}\n")
 
        # Reset all counters — next window starts from zero
        self._reset_window()
 
 
# entry main point
def main():
    '''
    create and run the analytics consumer
    
    '''
    consumer = AnalyticsConsumer()
    consumer.run()
    
if __name__ == "__main__":
    main()