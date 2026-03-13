# we dont run this file. we just import stuffs from here.

'''
base_consumer.py

purpose :
the shared foundation that all 3 consumers inherit from
contains everything that is same across analytic ,fraud and inventory consumers:
- connecting to kafka
- the poll loop : just a while loop that keeps asking kafka is there any message for me
- manual offset commits
- error handling
- dead letter routing
- shutdown

'''
from confluent_kafka import Consumer,Producer, KafkaException, KafkaError
# consumer : reads the events from kafka topics
# producer : writes dead letter events back to kafka

from config import (
    PRODUCER_CONFIG,
    TOPIC_DEAD_LETTER,
    CONSUMER_COMMIT_BATCH_SIZE, # COMMIT EVERY 100 messages
    CONSUMER_WINDOW_SECONDS # emit report every 10 seconds
)

# every commit is a network write to kafka
# commiting every single message : 6000 messages : 6000 network writes
# 100 messages : 60 network writes - 100 times less overhead

from models import deserialize # converts kafka bytes -> python dict
import time
import json
import threading # for shutdown event , stops the poll loop

class BaseConsumer:
    '''
    subclasses must implement
    - topics() -> list of topic names to subscribe to
    - process(event) -> business logic for event
    -emit_report() -> print the window summary

    '''
    def __init__(self,group_id:str, consumer_cfg:dict):
        '''
        initialize the base consumer
        group_id : consumer group id :

        '''
        # kafka consumer
        # creates the kafka consumer using the config
        # this will establish connection to cluster but does not start reading
        # reading starts when we call poll()
        self.consumer = Consumer(consumer_cfg)
        self.group_id = group_id
        # dead letter producer
        # small producer used only yo write unprocessable events
        dead_letter_config= {
            "bootstrap.servers" : PRODUCER_CONFIG["bootstrap.servers"],
            "client.id" : f"{group_id}-dead-letter-producer",
            "acks" : "1"
        }
        self.dead_letter_producer = Producer(dead_letter_config)
        # counters
        self.total_processed = 0
        self.total_errors = 0 # total events sends to the dead letter
        self.batch_count = 0 # events processed since the last commit
        # window timing
        self.window_start = time.time()
        # shutdown flag
        # threading.Event used to signal consumer to stop
        self.shutdown_event = threading.Event()

        print(f"[self.group_id] Consumer Initialized")
        print(f"[self.group_id] Subscribing to : {self.topics()}")
        print(f"[self.group_id] Commit batch size : {CONSUMER_COMMIT_BATCH_SIZE}")
        print(f"[self.group_id] Window size : {CONSUMER_WINDOW_SECONDS}s")
        # subscribe to the topics return by subclass topic()
        self.consumer.subscribe(self.topics())

    def topics(self)->list:
        '''
        Returns a list of kafka topics this consumer subscribes to
        must be implemented by every subclass
        list[str] : topic names
        return [TOPIC_USER_BEHAVIOR, TOPIC_ORDERS]

        '''
        raise NotImplementedError(
            f"{self.__class__.__name__} must implemented topics"
        )
    def process(self,event:dict,topic:str):
        '''
        process one event, Contains the business logic
        called by poll loop for every successfully deserialized event
        subclass decides what to do with the event - aggreagte it, check fraud rules, update inventory
        topic : which topic this event came from
        '''
        raise NotImplementedError(
            f"{self.__class__.__name__} must implemented topics"
        )

    def emit_report(self):
        '''
        print window summary report
        called every CONSUMER_WINDOWS_SECONDS by the poll loop
        '''
        raise NotImplementedError(
            f"{self.__class__.__name__} must implemented topics"
        )
    # Offset commit

    def _commit(self,force: bool=False):
        '''
        Commit the current offset to kafka if the batch size is reached
        called after every successful processed event

        '''
        if self.batch_count >= CONSUMER_COMMIT_BATCH_SIZE or force:
            try:
                # async=False means BLOCK until kafka confirms the commit
                self.consumer.commit(asynchronous=False)
                self.batch_count = 0

            except KafkaException as e:
                print(f"[{self.group_id} Commit failed : {e}]")

    # Dead letter routing
    def _dead_letter(self,raw_bytes:bytes, error:str, topic:str, offset:int):
        '''
        sends an unprocessable message to dead letter topic
        called when a message cannot be deserialized
        we route it to fluxcart.dead.letter

        dead letter event wraps the original bytes with :
        - what error occured
        - which topic and offset it came from
        - when it failed

        args :
        raw_bytes : original message bytes that could not be processed
        error : description of what went wrong
        topic : which topic the bad message came from
        offset : offset of bad message

        '''
        # build a dead letter envelope
        dead_letter_event = {
            "failed_at" : time.time(),
            "consumer_group" : self.group_id,
            "source_topic" : topic,
            "source_offset" : offset,
            "error": error,
            "original_message": (
                raw_bytes.decode("utf-8", errors="replace")
                if raw_bytes else "empty"

            )
        }

        try:
            # publish the dead leter event to the dead letter topic
            self.dead_letter_producer.produce(
                topic = TOPIC_DEAD_LETTER,
                value = json.dumps(dead_letter_event).encode("utf-8"),
            )
            # poll(0) : triggers the delivery callback without blocking
            self.dead_letter_producer.poll(0)
            print(f"[{self.group_id}] Dead letter: "
                  f"topic={topic} offset={offset} error={error[:50]}")

        except Exception as e:
            print(f"[{self.group_id}] Dead letter write failed : {e}")
    
    # Window management
    def _check_window(self):
        '''
        Check if the current window is elapsed and emit a report also

        '''
        if time.time() - self.window_start >= CONSUMER_WINDOW_SECONDS:
            self.emit_report()
            self.window_start = time.time() # reset the window timer

# =========================================================================
    # MAIN POLL LOOP — the heart of every Kafka consumer
    # =========================================================================
 
    def run(self):
        """
        Main consumer loop. Runs until shutdown_event is set or Ctrl+C.
 
        This is the most important method in the entire consumer codebase.
        Every Kafka consumer in production follows this exact pattern:
 
            while running:
                msg = poll()           ← ask Kafka for next message
                if no message: check window, continue
                if error: handle it
                deserialize the message
                process the message    ← subclass business logic
                commit offset          ← record our position
 
        The loop runs forever until:
            - Ctrl+C is pressed (KeyboardInterrupt)
            - shutdown_event.set() is called from run_pipeline.py
 
        In both cases, the finally block ensures:
            - Final offset commit (no messages re-processed on restart)
            - consumer.close() (releases partition assignments cleanly)
        """
 
        print(f"[{self.group_id}] Starting poll loop...")
 
        try:
            # Keep looping until the shutdown flag is set
            while not self.shutdown_event.is_set():
 
                # ── POLL ─────────────────────────────────────────────────
                # Ask Kafka for the next message.
                # timeout=1.0 means: wait UP TO 1 second for a message.
                # If a message arrives before 1 second, return it immediately.
                # If no message arrives within 1 second, return None.
                #
                # WHY 1 SECOND TIMEOUT?
                # We need to check the window timer and shutdown flag regularly.
                # A longer timeout means slower response to shutdown signals.
                # A shorter timeout means more CPU usage checking empty queues.
                # 1 second is the standard balance.
                msg = self.consumer.poll(timeout=1.0)
 
                # ── NO MESSAGE ───────────────────────────────────────────
                # poll() returned None — no new messages in this partition
                # right now. This is normal — topics are not always busy.
                if msg is None:
                    # Still check the window timer even with no messages
                    # (emit report on schedule regardless of message flow)
                    self._check_window()
                    continue   # go back to the top of the loop and poll again
 
                # ── KAFKA ERROR ──────────────────────────────────────────
                # The message has an error flag — something went wrong at
                # the Kafka level (not a processing error, a transport error).
                if msg.error():
 
                    # PARTITION_EOF is not a real error — it just means
                    # we have read all messages currently in this partition.
                    # More messages may arrive later. Just continue polling.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
 
                    # Any other Kafka error is a real problem — log it.
                    # We do not crash here — transient errors often resolve.
                    print(f"[{self.group_id}] Kafka error: {msg.error()}")
                    continue
 
                # ── PROCESS THE MESSAGE ───────────────────────────────────
                # We have a real message. Now:
                #   1. Deserialize bytes → Python dict
                #   2. Call the subclass's process() method
                #   3. Commit the offset
 
                try:
                    # Deserialize: convert raw bytes to Python dictionary
                    # msg.value() returns the raw bytes stored in Kafka
                    # deserialize() decodes UTF-8 and parses JSON
                    event = deserialize(msg.value())
 
                    # Call the subclass's business logic
                    # analytics.py, fraud.py, inventory.py each implement this
                    # msg.topic() tells the subclass which topic this came from
                    self.process(event, msg.topic())
 
                    # Increment counters
                    self.total_processed += 1
                    self.batch_count     += 1
 
                    # Commit offset if batch size reached
                    self._commit()
 
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    # The message bytes are not valid JSON or not valid UTF-8.
                    # This is a DATA error — the producer sent bad data.
                    # Route to dead letter instead of crashing.
                    self.total_errors += 1
                    self._dead_letter(
                        raw_bytes = msg.value(),
                        error     = f"Deserialization error: {e}",
                        topic     = msg.topic(),
                        offset    = msg.offset(),
                    )
 
                except Exception as e:
                    # Any other processing error — business logic crashed.
                    # Route to dead letter instead of crashing the consumer.
                    # The consumer continues with the next message.
                    self.total_errors += 1
                    self._dead_letter(
                        raw_bytes = msg.value(),
                        error     = f"Processing error: {e}",
                        topic     = msg.topic(),
                        offset    = msg.offset(),
                    )
 
                # Check window timer after every message
                self._check_window()
 
        except KeyboardInterrupt:
            # Ctrl+C was pressed — stop gracefully
            print(f"\n[{self.group_id}] Ctrl+C received — shutting down...")
 
        finally:
            # ── ALWAYS RUN THIS BLOCK ─────────────────────────────────────
            # Whether we stopped via Ctrl+C, shutdown_event, or an exception,
            # we must always:
            #   1. Emit the final partial window report
            #   2. Commit the final partial batch
            #   3. Close the consumer connection
 
            # Emit whatever is in the current window
            if self.total_processed > 0:
                self.emit_report()
 
            # Force-commit the final partial batch
            # (batch_count might be < CONSUMER_COMMIT_BATCH_SIZE)
            # Without this, the last few messages would be reprocessed
            # on restart even though we already handled them.
            self._commit(force=True)
 
            # Flush any pending dead letter messages
            self.dead_letter_producer.flush()
 
            # Close the consumer — releases partition assignments back to
            # the group coordinator. Other consumers in the group can then
            # pick up these partitions immediately rather than waiting for
            # the session timeout (30 seconds) to expire.
            self.consumer.close()
 
            # Final summary
            print(f"[{self.group_id}] Stopped.")
            print(f"[{self.group_id}] Total processed : {self.total_processed}")
            print(f"[{self.group_id}] Total errors    : {self.total_errors}")
 
    def stop(self):
        """
        Signal this consumer to stop gracefully.
 
        Called by run_pipeline.py when Ctrl+C is pressed.
        Sets the shutdown_event flag which the poll loop checks
        at the top of every iteration.
 
        The consumer finishes processing its current message,
        commits the final batch, and exits cleanly.
        """
        print(f"[{self.group_id}] Stop signal received...")
        self.shutdown_event.set()   # signal the poll loop to exit

