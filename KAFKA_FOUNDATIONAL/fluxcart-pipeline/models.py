'''
do we run models.py ? we dont
same as config.py
Consumers -- import it to understand events they receive

purpose :
defines exactly how each event in the pipeline looks like
3 types of events , each defined as a python dataclass
- UserBehaviorEvent(browsing,searching,add to cart)
- OrderEvent (order placed, confirmed,shipped,delivered)
- PaymentEvent (payment initiated, success, failed)

why this file ?
before writing a single producer or consumer, you need to agree upon
what an event looks like - what field does it have, what types , partition key
file agreement - between producer and consumer

models.py. : producer.py file import it to generate events
consumers import it to understand the event structure
'''

import uuid   # help you generate unique event IDs
import random
import json
from dataclasses import dataclass 
from datetime import datetime
from typing import Optional   # pydantic data validation

# generate fake data
# 50 simulated users - user IDs -- user 0001 to user 0050

USERS = [f"user-{str(i).zfill(4)}" for i in range(1,51)]

# 20 simulated products across 5 categories
PRODUCTS = {
    # category : list of product IDs in that category
    # electronics : elec-001, elec-005
    "electronics" : [f"elec-{i:03d}" for i in range(1,6)],
    "clothing" : [f"clth-{i:03d}" for i in range(1,6)],
    "books" : [f"book-{i:03d}" for i in range(1,6)],
    "home" : [f"home-{i:03d}" for i in range(1,6)],
    "sports" : [f"sprt-{i:03d}" for i in range(1,6)],
}

# flat list of all products IDs
ALL_PRODUCTS = [p for products in PRODUCTS.values() for p in products]

# flat list of all categories
ALL_CATEGORIES = list(PRODUCTS.keys())

# devices user brows and buy from
DEVICES =["web","mobile-ios","mobile-android","tablet","smart-tv"]

# Countries
COUNTRIES = ["IN","US","GB","DE","SG","AE","AU","CA"]

# payment methods
PAYMENT_METHODS = ["credit_card","debit_card","upi","netbanking","wallet"]

# warehouses
WAREHOUSES = ["wh-mumbai","wh-delhi","wh-bangalore","wh-chennai","wh-hyderabad"]

# price ranges per category (min,max)
PRICE_RANGES = {
    "electronics" : (5000,150000),
    "clothing" : (500,10000),
    "books" : (200,2000),
    "home" : (100,50000),
    "sports" : (800,30000),

}

#########################
# Event 1 - UserBehaviorEvent
# every action a user takes while browsing fluxcart
# these are high volume events - every scroll, search, click

# partition key : user_id

# all the events for the same user always land in the same partition

#########################

@dataclass # decorator is used for validation and type checking here. if you give int for event_id, it will throw an error
class UserBehaviorEvent:
    """
    represents one user action on the fluxcart platform
    produced by : producer.py
    consumer by : analytics consumer (to track funnels, top products, sessions)
    topic : fluxcart.user.behavior
    partition_key : user_id
    """
    # every event has some universal fields
    event_id : str
    user_id : str
    timestamp : str
    # what did the user actually do
    action : str  # view, search, add_to_cart, remove_from_cart,wishlist
    # what they interacted with
    product_id : str
    category : str
    price : float
    # context about how and where
    device : str # web, phone, tablet
    country : str
    # session tracking - group events from the same browsing session
    # format : "session-{user_id}-{session_number}"
    # user-0023 might have session-user-0023-2
    session_id : str

    @classmethod
    def generate(cls) -> "UserBehaviorEvent":
        '''
        Create and return fake User behvaiour event

        '''
        user_id = random.choice(USERS)
        category = random.choice(ALL_CATEGORIES)
        product_id = random.choice(PRODUCTS[category])
        # get the price range
        min_price, max_price = PRICE_RANGES[category]
        price = round(random.uniform(min_price,max_price),2)
        # weighted distribution
        action = random.choices(
            population=["view","search","add_to_cart","remove_from_cart","wishlist"],
            weights = [50,20,15,5,10],
            k = 1
        )[0]
        # session_id
        session_number = random.randint(1,3)
        session_id = f"session-{user_id}-{session_number}"
        return cls(
            event_id = str(uuid.uuid4()),
            user_id = user_id,
            timestamp = datetime.utcnow().isoformat() + "Z",
            action = action,
            product_id = product_id,
            category = category,
            price = price,
            device = random.choice(DEVICES),
            country =  random.choice(COUNTRIES),
            session_id = session_id

        )

    def to_bytes(self) -> bytes:
        '''
        serialize this event to JSON
        kafka stores raw bytes - it does not understand python objects
        this method convert the dataclass to JSON string, then encodes
        that JSON string to bytes using UTF-8 encoding
        returns : the event as UTF-8 encoded JSON, ready for kafka

        '''
        return json.dumps(vars(self)).encode("utf-8")
        # before : {"event_id":"abc" , user_id : "user-0023"}
        # after : b"{"event_id":"abc" , user_id : "user-0023"}""

    @property
    def partition_key(self) -> bytes:
        '''
        partition key of this event - encoded as bytes
        kafka hash this key to decide which partition this event goes to

        '''
        return self.user_id.encode("utf-8")
        # .encode("utf-8") converts string "user-0023" to bytes b"user-0023"

        # =============================================================================
# EVENT 2 — OrderEvent
# =============================================================================
#
# WHAT IT REPRESENTS:
#   Every state change in an order's lifecycle.
#   An order goes through: PLACED → CONFIRMED → PACKED → SHIPPED → DELIVERED
#   Each state change is a NEW event — we never update old events.
#
# PARTITION KEY: order_id
#
#   WHY order_id AS THE KEY?
#   All events for the same order always land in the same partition, in order.
#   This guarantees:
#     1. PLACED always comes before CONFIRMED in the partition
#     2. CONFIRMED always comes before SHIPPED
#     3. The inventory consumer sees events in the right sequence
#
#   If order_id was NOT the key, DELIVERED might arrive before PLACED
#   in a partition — your inventory logic would break completely.
#
#   IMPORTANT: PaymentEvent also uses order_id as its key.
#   This means order events and payment events for the same order
#   land in the same partition NUMBER across different topics.
#   This makes joining them in the fraud consumer natural.

@dataclass
class OrderEvent:
    """
    Represents one state change in a FluxCart order's lifecycle.

    Produced by: producer.py
    Consumed by: analytics consumer (conversion funnel)
                 inventory consumer (stock management)
                 fraud consumer (order pattern analysis)
    Topic:       fluxcart.orders
    Partition key: order_id
    """

    # Universal fields
    event_id:     str    # unique ID for this specific event
    timestamp:    str    # when this state change happened

    # Order identity
    order_id:     str    # unique order ID — PARTITION KEY
                         # format: "ord-{uuid4_short}"
    user_id:      str    # who placed this order

    # What was ordered
    product_id:   str    # which product
    category:     str    # product category
    quantity:     int    # how many units
    unit_price:   float  # price per unit at time of order (INR)
    order_total:  float  # quantity × unit_price

    # Order lifecycle state
    # Each state transition produces a new OrderEvent with the new status
    status:       str    # "placed" | "confirmed" | "packed" | "shipped"
                         # | "delivered" | "cancelled" | "returned"

    # Fulfillment details
    warehouse_id: str    # which warehouse is fulfilling this order

    @classmethod
    def generate(cls, status: str = "placed") -> "OrderEvent":
        """
        Generate a realistic fake OrderEvent.

        Args:
            status: the order lifecycle status for this event.
                    Defaults to "placed" — most generated events are new orders.

        Returns:
            OrderEvent: a fully populated event ready to be published
        """

        # Pick a random user and product
        user_id   = random.choice(USERS)
        category  = random.choice(ALL_CATEGORIES)
        product_id = random.choice(PRODUCTS[category])

        # Generate a short order ID — readable but unique enough for simulation
        # In production this would come from your order management system
        order_id = f"ord-{str(uuid.uuid4())[:8]}"

        # Pick quantity (most orders are 1-2 items, occasionally more)
        quantity = random.choices(
            population=[1, 2, 3, 4, 5],
            weights   =[50, 30, 10, 7, 3],   # 50% of orders are single item
            k=1
        )[0]

        # Calculate pricing
        min_price, max_price = PRICE_RANGES[category]
        unit_price  = round(random.uniform(min_price, max_price), 2)
        order_total = round(unit_price * quantity, 2)

        return cls(
            event_id     = str(uuid.uuid4()),
            timestamp    = datetime.utcnow().isoformat() + "Z",
            order_id     = order_id,
            user_id      = user_id,
            product_id   = product_id,
            category     = category,
            quantity     = quantity,
            unit_price   = unit_price,
            order_total  = order_total,
            status       = status,
            warehouse_id = random.choice(WAREHOUSES),
        )

    def to_bytes(self) -> bytes:
        """Serialize this event to JSON bytes for Kafka."""
        return json.dumps(vars(self)).encode("utf-8")

    @property
    def partition_key(self) -> bytes:
        """
        Partition key = order_id.
        All events for the same order go to the same partition, in order.
        """
        return self.order_id.encode("utf-8")
    
    # =============================================================================
# EVENT 3 — PaymentEvent
# =============================================================================
#
# WHAT IT REPRESENTS:
#   Every state change in a payment's lifecycle.
#   A payment goes through: INITIATED → SUCCESS or FAILED
#   A failed payment might be retried: INITIATED → FAILED → INITIATED → SUCCESS
#
# PARTITION KEY: order_id  (NOT payment_id)
#
#   WHY order_id AND NOT payment_id?
#   Because the fraud consumer needs to correlate payment events with
#   order events for the same order. Both use order_id as their key,
#   so they land in the same partition number across their respective topics.
#
#   If we used payment_id as the key, a payment event for order ORD-001
#   might be in partition 2, but the order event for ORD-001 might be
#   in partition 5. The fraud consumer cannot easily join them.
#
#   Using the same key across topics = natural alignment of related events.
#
# FRAUD DETECTION FIELDS:
#   account_age_days — how old is this user's account?
#   A new account (< 7 days) making a large payment is a fraud signal.
#   This field is attached to every payment event so the fraud consumer
#   does not need to look it up from a database.

@dataclass
class PaymentEvent:
    """
    Represents one state change in a FluxCart payment's lifecycle.

    Produced by: producer.py
    Consumed by: fraud consumer (fraud detection rules)
    Topic:       fluxcart.payments
    Partition key: order_id (NOT payment_id — see explanation above)
    """

    # Universal fields
    event_id:         str    # unique ID for this specific event
    timestamp:        str    # when this payment state change happened

    # Payment identity
    payment_id:       str    # unique payment ID — format: "pay-{uuid4_short}"
    order_id:         str    # which order this payment is for — PARTITION KEY
    user_id:          str    # who is paying

    # Payment details
    amount:           float  # payment amount in INR
    currency:         str    # always "INR" for FluxCart India
    payment_method:   str    # "credit_card" | "debit_card" | "upi" | etc.

    # Payment lifecycle state
    status:           str    # "initiated" | "success" | "failed" | "refunded"

    # Fraud detection fields — attached at event creation time
    # so fraud consumer does not need a separate database lookup
    account_age_days: int    # how many days old is this user's account?
                             # < 7 days = new account (fraud signal)
                             # > 365 days = established account (lower risk)

    @classmethod
    def generate(cls, status: str = "initiated") -> "PaymentEvent":
        """
        Generate a realistic fake PaymentEvent.

        Args:
            status: payment lifecycle status. Defaults to "initiated".

        Returns:
            PaymentEvent: a fully populated event ready to be published
        """

        user_id  = random.choice(USERS)
        order_id = f"ord-{str(uuid.uuid4())[:8]}"   # matches OrderEvent format

        # Payment amounts — most payments are moderate, some are large
        # Using exponential-ish distribution: many small, few large
        amount = round(random.choices(
            population=[
                random.uniform(200, 2000),       # small purchase
                random.uniform(2000, 20000),     # medium purchase
                random.uniform(20000, 100000),   # large purchase
                random.uniform(100000, 500000),  # very large — fraud signal
            ],
            weights=[60, 30, 8, 2],              # 60% small, 2% very large
            k=1
        )[0], 2)

        # Account age — most users are established, some are new
        account_age_days = random.choices(
            population=[
                random.randint(0, 6),      # new account (0-6 days)
                random.randint(7, 30),     # recent account (1-4 weeks)
                random.randint(31, 365),   # regular user (1-12 months)
                random.randint(366, 1500), # long-term user (1-4 years)
            ],
            weights=[5, 15, 40, 40],       # 5% are brand new accounts
            k=1
        )[0]

        return cls(
            event_id         = str(uuid.uuid4()),
            timestamp        = datetime.utcnow().isoformat() + "Z",
            payment_id       = f"pay-{str(uuid.uuid4())[:8]}",
            order_id         = order_id,
            user_id          = user_id,
            amount           = amount,
            currency         = "INR",
            payment_method   = random.choice(PAYMENT_METHODS),
            status           = status,
            account_age_days = account_age_days,
        )

    def to_bytes(self) -> bytes:
        """Serialize this event to JSON bytes for Kafka."""
        return json.dumps(vars(self)).encode("utf-8")

    @property
    def partition_key(self) -> bytes:
        """
        Partition key = order_id.
        Aligns payment events with order events for the same order
        across different topics (both use order_id as key).
        """
        return self.order_id.encode("utf-8")
    
# Defining the Deserializer function
# when a consumer reads a message from kafka, it receives raw bytes
# this function convert those bytes back into py dict
# consumers use this to access individual fields like "user_id"
    
def deserialize(raw_bytes : bytes) -> dict:
    '''
    converts raw kafka messages bytes back to dictionary
    this is revers of to_bytes() called by every consumer
    when it receives a message from kafka
    args : raw_bytes received from kafka message.value()
    returns : dict event
    raises an error : json.JSONDecodeError : if the bytes are not valid JSON
    (trigger dead letters in consumers)

    raw_bytes.decode("utf-8") : converts bytes back to python string
    # b'{"event_id: "abc"}' ---> "{"event_id":"abc"}"
    # json.loads.....
    # {"event_id":"abc"}

    # consumer ----> event = deserialize(msg.value())
    # print(event["user_id"]) --> "user-0023"
    '''
    return json.loads(raw_bytes.decode("utf-8"))
    
    

