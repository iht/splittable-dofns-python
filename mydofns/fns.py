import sys
import time
from typing import Optional, List, Iterable, Tuple

import apache_beam as beam
from apache_beam import RestrictionProvider, coders
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker, UnsplittableRestrictionTracker
from apache_beam.io.watermark_estimators import MonotonicWatermarkEstimator, WalltimeWatermarkEstimator
from apache_beam.transforms.core import ProcessContinuation
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.transforms.window import TimestampedValue
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata


# Check what are the partitions that we can try to read from Kafka

# This is a normal DoFn. Here Dataflow may run several instances in parallel, all of them "peeking" from Kafka.
# Since this is an idempotent DoFn, this is not an issue. Even if there is parallel processing, if we add a Reshuffle
# step afterwards, only one version of the processing "will win", so we will process each partition only once.
#
# But need to be careful with normal DoFns. Each incoming element may be processed more than once, and there is no way
# to prevent Dataflow from doing that additional processing (e.g. this happens when doing autoscaling).
#
# If we want to be able to process in parallel and with guarantees of "locks" for non-idempotent transforms, we need to
# use Splittable DoFns
class PartitionWatcherDoFn(beam.DoFn):
    NUMBER_OF_CONSUMERS = 5

    def __init__(self, bootstrap_servers: List[str], *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self._bootstrap_servers = bootstrap_servers

    # We receive ints from the periodic impulse
    def process(self,
                element: Tuple[str, int],
                **unused_kwargs) -> Iterable[int]:
        topic, _ = element

        # seen_partitions = state.read()
        for consumer_id in range(self.NUMBER_OF_CONSUMERS):
            yield consumer_id


# This is a non-idempotent DoFn, because we commit the offsets to Kafka when we have processed them fully.
# Here we have guarantees that for each incoming element (partition id), we only process each offset exactly once, and
# that any other potential instance of the same DoFn attempting to process that same offset will not be able to do it.
#
# For that, it is very important that any non-idempotent operation (e.g. a commit) happens only when we have obtained
# a claim in that offset.
#
# Also, it is essential that once we have claimed an offset, we ensure that the offset is fully processed.
# For that, we have included here a blocking commit operation. The DoFn will only finish processing a partition when it
# is able to commit all the offsets that it has claimed.


class MyKafkaRestriction(OffsetRange):
    def __init__(self, start, stop, partition):
        print(f"Creating restriction {start}  {stop}  {partition}")
        super().__init__(start, stop)
        self._partition = partition

    def get_partition(self):
        return self._partition


class MyKafkaRestrictionTracker(OffsetRestrictionTracker):
    def __init__(self, offset_range, bootstrap_server: List[str], topic: str):
        super().__init__(offset_range)
        self._consumer = KafkaConsumer(group_id=ReadFromKafkaCustomDoFn.MAIN_CONSUMER_GROUP,
                                       bootstrap_servers=bootstrap_server,
                                       auto_offset_reset='earliest',
                                       enable_auto_commit=False)

        self._topic = topic

    def check_done(self):
        print("Check done")
        r = self.current_restriction()
        partition = r.get_partition()
        right_restriction_limit = r.stop
        tp = TopicPartition(topic=self._topic, partition=partition)

        last_committed_offset: int = self._consumer.committed(tp)

        if last_committed_offset is not None and last_committed_offset <= right_restriction_limit:
            return True

        return False

    def is_bounded(self):
        print("Is bounded")
        return False

    def try_split(self, fraction_of_remainder):
        print("Try split")
        # Return tuple with two ranges: fully processed, rest to be processed in the future
        partition = self.current_restriction().get_partition()
        tp = TopicPartition(topic=self._topic, partition=partition)

        last_committed_offset: int = self._consumer.committed(tp)

        committed_range = MyKafkaRestriction(self.current_restriction().start, last_committed_offset, partition)
        to_be_proc_range = MyKafkaRestriction(last_committed_offset + 1, sys.maxsize, partition)

        return committed_range, to_be_proc_range


class ReadFromKafkaCustomDoFn(beam.DoFn, RestrictionProvider):
    MAIN_CONSUMER_GROUP = "consumer-group"
    POLL_MAX_RECORDS = 50

    def __init__(self, topic_name: str, bootstrap_server: List[str], poll_interval: int, *unused_args, **unused_kwargs):
        print("DoFn creating")
        super().__init__(*unused_args, **unused_kwargs)
        self._topic = topic_name
        self._bootstrap_servers = bootstrap_server
        self._poll_interval = poll_interval
        self._consumer: Optional[KafkaConsumer] = None  # for processing and committing messages

    def _create_consumer(self, group: str) -> KafkaConsumer:
        return KafkaConsumer(group_id=group,
                             bootstrap_servers=self._bootstrap_servers,
                             auto_offset_reset='earliest',
                             enable_auto_commit=False)

    def setup(self):
        if self._consumer is None:
            self._consumer = self._create_consumer(self.MAIN_CONSUMER_GROUP)

    # We need to claim the full partition, and keep processing that partition forever
    @beam.DoFn.unbounded_per_element()
    def process(self,
                partition: int,
                tracker=beam.DoFn.RestrictionParam(),
                **unused_kwargs):
        tp = TopicPartition(topic=self._topic, partition=partition)
        print(f"Processing partition {tp}")
        self._consumer.assign([tp])
        while True:
            batch = self._consumer.poll()

            initial = self._consumer.committed(tp)
            if batch:
                # Exactly once processing with parallelization: only commit what you are able to claim
                # Only commit the offsets we can claim
                print(f"Processing partition {partition}")

                values = batch[tp]

                n = 0
                offset_to_commit = 0
                for v in values:
                    n += 1
                    # For testing purposes, let's print this other info (instead of v.value)
                    if tracker.try_claim(v.offset):
                        msg = f"Partition: {partition}  Offset: {v.offset}"
                        # watermark = watermark_estimator.current_watermark()  # setting the watermark
                        yield msg
                        offset_to_commit = v.offset + 1  # this is the next message to be consumed
                    else:
                        print(f"Claim lost in partition {partition}  offset {v.offset}.")
                        print(f"{n} messages processed")
                        # Commit first and then return
                        self._consumer.commit({tp: OffsetAndMetadata(offset_to_commit, "")})
                        return

                # Commit processed messages
                self._consumer.commit({tp: OffsetAndMetadata(offset_to_commit, "")})
                print(f"{n} messages processed")
            else:
                last = self._consumer.end_offsets([tp])[tp]
                print(f"No messages found in partition {partition} (offset last: {last}, initial: {initial})")

                time.sleep(self._poll_interval)

    def teardown(self):
        del self._consumer

    ## Restriction provider methods
    # For SDF, we need to override these:
    def create_tracker(self, restriction: MyKafkaRestriction) -> MyKafkaRestrictionTracker:
        return MyKafkaRestrictionTracker(restriction, bootstrap_server=self._bootstrap_servers, topic=self._topic)

    def initial_restriction(self, partition: int) -> MyKafkaRestriction:
        if self._consumer is None:
            self._consumer = self._create_consumer(self.MAIN_CONSUMER_GROUP)

        tp = TopicPartition(topic=self._topic, partition=partition)
        initial = self._consumer.committed(tp)
        if initial is None:
            initial = 0
        return MyKafkaRestriction(start=initial, stop=initial + self.POLL_MAX_RECORDS, partition=partition)

    def restriction_size(self, partition: int, restriction: MyKafkaRestriction) -> int:
        # print(f"Restriction size is {restriction.size()}")
        return restriction.size()
