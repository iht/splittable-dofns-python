import sys
import time
from typing import List, Optional, Iterable

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.iobase import RestrictionTracker, RestrictionProgress
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from kafka import KafkaConsumer

import logging

class ConsumerRestriction:
    def __init__(self, consumer_id: int):
        self._consumer_id = consumer_id
        self._completed = 0
        self._draining_has_been_called = False

    def get_id(self) -> int:
        return self._consumer_id

    def beware_we_are_draining(self):
        self._draining_has_been_called = True

    def are_we_draining(self):
        return self._draining_has_been_called

    def add_completed(self, n: int):
        self._completed += n

    def get_completed(self) -> int:
        return self._completed


class ConsumerRestrictionTracker(RestrictionTracker):
    def __init__(self, restriction: ConsumerRestriction):
        self._restriction = restriction

    def current_restriction(self) -> ConsumerRestriction:
        return self._restriction

    def current_progress(self) -> RestrictionProgress:
        return RestrictionProgress(completed=self._restriction.get_completed(), fraction=0, remaining=sys.maxsize - 1)

    def check_done(self):
        return False  # a partition is never done

    def try_split(self, fraction_of_remainder):
        return  # A partition(s) cannot be splitted

    def try_claim(self, position: int):
        # Make sure we never get someone else claim our consumer id
        return position == self._restriction.get_id()

    def is_bounded(self):
        return False  # This is an unbounded tracker


class ConsumerRestrictionProvider(RestrictionProvider):
    def create_tracker(self, restriction: ConsumerRestriction) -> ConsumerRestrictionTracker:
        return ConsumerRestrictionTracker(restriction=restriction)

    def initial_restriction(self, consumer_id: int) -> ConsumerRestriction:
        return ConsumerRestriction(consumer_id=consumer_id)

    def restriction_size(self, partition: int, restriction: ConsumerRestriction) -> int:
        return 1

    def truncate(self, element, restriction: ConsumerRestriction):
        # This method is called when draining, we should make sure that we stop in the next iteration
        restriction.beware_we_are_draining()
        return restriction


class ReadFromKafkaCustomDoFnWithPartition(beam.DoFn, RestrictionProvider):
    CONSUMER_GROUP = "my-dataflow-consumer-group"
    POLL_MAX_RECORDS = 50

    def __init__(self, topic_name: str, bootstrap_server: List[str], poll_interval: int, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self._topic = topic_name
        self._bootstrap_servers = bootstrap_server
        self._poll_interval = poll_interval

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(group_id=self.CONSUMER_GROUP,
                             bootstrap_servers=self._bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False)

    # We need to claim the full partition, and keep processing that partition forever
    @beam.DoFn.unbounded_per_element()
    def process(self,
                consumer_id: int,
                tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(ConsumerRestrictionProvider()),
                watermark=beam.DoFn.WatermarkEstimatorParam(WalltimeWatermarkEstimator.default_provider()),
                **unused_kwargs) -> Iterable[str]:

        if tracker.try_claim(consumer_id):
            consumer = self._create_consumer()  # KafkaConsumer is non-serializable, so let's create here
            # We will be creating one per consumer_id
            consumer.subscribe([self._topic])
        else:
            logging.info(f"Claim lost with consumer id {consumer_id}")
            return

        while not tracker.current_restriction().are_we_draining():
            logging.info(f"Polling consumer_id {consumer_id}")
            batch = consumer.poll()
            if batch:
                logging.info(f"Processing consumer_id {consumer_id}")
                for tp, values in batch.items():
                    values = batch[tp]
                    n = 0
                    for v in values:
                        n += 1
                        # For testing purposes, let's print this other info (instead of v.value)
                        msg = f"Partition: {tp.partition}  Offset: {v.offset}"
                        yield msg
                    logging.info(f"    {n} messages processed in partition {tp.partition}")
                    r: ConsumerRestriction = tracker.current_restriction()
                    r.add_completed(n)

                # Commit processed messages
                consumer.commit()
            else:
                logging.info(f"No messages found with consumer {consumer_id}")
                time.sleep(self._poll_interval)
