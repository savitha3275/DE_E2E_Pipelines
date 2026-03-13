'''
topic creation script

PRODUCER WRITES TO A TOPIC
CONSUMER READS FROM A TOPIC
BROKER STORES THE TOPIC


purpose:
creates all kafka topics that the pipeline needs
this is the script you run before the producer , before any consumer
topics must exist before anything can write to or read from them

when to run ? once before running the pipeline for the first time
if you run it again, it ill tell you the topics already exist
and skip them, it will not duplicate or overwrite anything

what it creates ?
fluxcart.us# It seems like there is a typo in the topic name mentioned in the script. The correct
# topic name should be `fluxcart.user.behavior` instead of `fluxcart.er.behavior`.
er.behavior - 6 partitions , 30 days retention
fluxcart.orders - 3 partitions, 90 day retention
fluxcart.payments - 3 partitions , 90 day retention
fluxcart.fraud.alerts - 1 partition, 30 day retention
fluxcart.dead.letter - 1 partition , 30 day retention

topic is an ordered list of messages , stored on disk

fluxcart.orders - messages list:

(OFFSET)position 0 : {"order_id": 001, "status":"confirmed","user_id","user-0023"}:

topics versus partitions ?
a topic as name of channel : fluxcart.orders is a topic

a partition is how a topic is split into parallel lanes
fluxcart.orders
--- Partition 0 --> [mesg0,mesg3,mesg6]
--- Partition 1 --> [mesg1,mesg4,mesg7]
--

requires : kafka cluster runnning  : docker compose up -d
pip install confluent-kafka
'''

from confluent_kafka.admin import AdminClient,NewTopic
# admin client - connects to kafka for admin tasks : create topics / delete topics
# NewTopic : describes a topic we want to create (name, partitions, config)
from confluent_kafka import KafkaException
from config import (
    KAFKA_BOOTSTRAP_SERVERS,  # localhost:9092
    TOPIC_CONFIGS, # dict of all topic names and their settings
)
import sys # sys.exit() - exit with error code if the setup fail
import time # time.sleep()

# connect to kafka cluster

def create_admin_client() -> AdminClient:
    '''
    Admin client is a special kafka client used only for admin tasks
    it is NOT. used for producing or consuming events
    returns : connected admin client ready to manage topics
    raises : SystemExit
    '''
    print(f"Connecting to kafka at {KAFKA_BOOTSTRAP_SERVERS}.....")
    # admin client only needs the bootstrap server address
    admin = AdminClient({
        "bootstrap.servers" : KAFKA_BOOTSTRAP_SERVERS,
    })
    print("Connected.\n")
    return admin

# Checking which topics already exist

def get_existing_topics(admin: AdminClient) -> set:
    '''
    returns a set of topic names that already exist on the cluster
    we check this before creating any topics sp we can skip the ones that
    already exist and only create the ones that are missing

    args : admin : connected Admin Client
    returns : set like : {"fluxcart.orders"}
    '''
    # list_topics() fetches metadata for the entire cluster
    # .topics is a dict where keys are topic names
    # timeout = 10
    cluster_metadata = admin.list_topics(timeout=10)
    # extract topic names into a set for faster lookup
    existing = set(cluster_metadata.topics.keys())
    # filter out kafka's internal topics (they start with double underscore)
    # e.g : __consumer_offsets,
    existing = {t for t in existing if not t.startswith("__")}
    return existing


# build the list of topics to create

def build_new_topics(existing_topics:str) -> list:
    '''
    build a list of new topic objects for topics that does not exist yet
    skips any topics that already exists - safe to run multiple times
    args : existing_topics :
    returns : list of new topics : topics that need to be created
    '''
    topics_to_create = []
    # loop through every topic defined in config.py
    for topic_name,topic_config in TOPIC_CONFIGS.items():
        # skip this topic if already exists on the cluster
        if topic_name in existing_topics:
            print(f"SKIP {topic_name} (already exists)")
            continue

        # build a new topic object with the settings from config.py
        new_topic = NewTopic(
            topic = topic_name,
            num_partitions=topic_config["num_partitions"],
            replication_factor=topic_config["replication_factor"],
            # additional topic level configuration as a dict of strings
            config={
                "retention.ms":str(topic_config["retention_ms"]),
                "cleanup.policy":"delete",
                "min.insync.replicas" : 2
            }

        )

        topics_to_create.append(new_topic)
        print(f"  PENDING  {topic_name} "
                f"({topic_config['num_partitions']} partitions, "
                f"replication={topic_config['replication_factor']}, "
            f"retention={topic_config['retention_ms'] // (24*60*60*1000)}d)")
    return topics_to_create


# Send the Creation requests to kafka

def create_topics(admin: AdminClient, topics_to_create: list) -> bool:
    """
    Send topic creation requests to Kafka and wait for results.

    create_topics() is asynchronous — it returns futures immediately.
    We must call future.result() on each one to wait for completion
    and check if it succeeded or failed.

    Args:
        admin:            connected AdminClient
        topics_to_create: list of NewTopic objects to create

    Returns:
        bool: True if all topics created successfully, False if any failed
    """

    if not topics_to_create:
        # Nothing to create — all topics already exist
        return True

    print(f"\nCreating {len(topics_to_create)} topic(s)...")

    # create_topics() sends creation requests to Kafka.
    # Returns a dict mapping topic_name -> Future object.
    # A Future represents a result that is not ready yet.
    futures = admin.create_topics(topics_to_create)

    all_succeeded = True   # track if any topic creation failed

    # Wait for each future to complete and check the result
    for topic_name, future in futures.items():
        try:
            # future.result() BLOCKS until Kafka responds.
            # If creation succeeded, returns None.
            # If creation failed, raises an exception.
            future.result()
            print(f"  CREATED  {topic_name}")

        except KafkaException as e:
            # Check if the error is just "topic already exists"
            # This can happen in race conditions — not a real error
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"  EXISTS   {topic_name} (created concurrently)")
            else:
                # A real error — log it and mark as failed
                print(f"  FAILED   {topic_name}: {e}")
                all_succeeded = False

    return all_succeeded



# Verify if the topics that were created correctly or not

def verify_topics(admin: AdminClient) -> None:
    '''
    list all the topics on the cluster and print a summary table
    called after creation to confirm everything looks right
    shows topic_name, partition_count, and replication for each topic
    '''
    print("TOPIC VERIFICATION......")
    # fetch fresh cluster metadata
    metadata = admin.list_topics(timeout=10)

    # sort the topics alphabetically , skip internal kafka topics
    fluxcart_topics = sorted([
        name for name in metadata.topics.keys()
        if name.startswith("fluxcart")
    ])

    if not fluxcart_topics:
        print("No fluxcart topics found. Something went wrong")
        return

    print(f"{'TOPIC'} {'PARTITIONS'} {'REPLICAS'}")
    # print 1 row per topic
    for topic_name in fluxcart_topics:
        topic_metadata = metadata.topics[topic_name]
        # number of partitions = number of entries in topic.metadata.partitions
        num_partitions = len(topic_metadata.partitions)
        # number of replicas = number of replicas on partiton 0
        # all partition have same replication factor , so checking partition 0 is sufficient
        parition_0 = topic_metadata.partitions[0]
        num_replicas = len(parition_0.replicas)
        print(f"{topic_name} {num_partitions} {num_replicas}")

    print(f"Total Fluxcart topics : {len(fluxcart_topics)}")
    print("Cluster is ready. You can now run the producer and consumer")



def main():
    """
    Main function — orchestrates the full topic setup process.

    Steps:
        1. Connect to Kafka cluster
        2. Check which topics already exist
        3. Build list of topics that need creating
        4. Create the missing topics
        5. Verify everything looks correct
    """

    print("="*60)
    print("FluxCart — Kafka Topic Setup")
    print("="*60 + "\n")

    # Step 1 — connect
    admin = create_admin_client()

    # Step 2 — find existing topics
    print("Checking existing topics...")
    existing = get_existing_topics(admin)

    if existing:
        print(f"  Found {len(existing)} existing topic(s) on cluster")
    else:
        print("  No existing topics found — fresh cluster")

    print()

    # Step 3 — build list of topics to create
    print("Planning topic creation:")
    topics_to_create = build_new_topics(existing)

    # Step 4 — create the missing topics
    success = create_topics(admin, topics_to_create)

    if not success:
        # At least one topic failed to create — exit with error code 1
        # Error code 1 signals failure to any script that called this one
        print("\nSetup failed. Fix the errors above and try again.")
        sys.exit(1)

    # Brief pause — give Kafka a moment to finalize partition assignments
    # before we query the metadata for verification
    time.sleep(2)

    # Step 5 — verify
    verify_topics(admin)


# =============================================================================
# ENTRY POINT
# =============================================================================
# This block only runs when you execute: python setup_topics.py
# It does NOT run when another file does: from setup_topics import something
# This pattern is standard in every Python script.

if __name__ == "__main__":
    main()