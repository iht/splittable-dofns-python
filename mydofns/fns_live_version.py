import datetime
import sys
import time
from typing import Tuple, Optional, List

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.coders import coders
from apache_beam.io import OffsetRangeTracker
from apache_beam.io.restriction_trackers import OffsetRange, UnsplittableRestrictionTracker, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator, MonotonicWatermarkEstimator
from apache_beam.transforms.userstate import BagStateSpec
from kafka import KafkaConsumer, TopicPartition


class PartitionWatcherDoFn(beam.DoFn):
    SEEN_PARTITIONS = BagStateSpec('seen-partitions', coders.VarIntCoder())

    def __init__(self, bootstrap_server):
        self._bootstrap_server = bootstrap_server
        self._consumer: Optional[KafkaConsumer] = None

    def setup(self):
        self._consumer = KafkaConsumer(group_id="read_partitions",
                                       bootstrap_servers=self._bootstrap_server)

    def process(self,
                t: Tuple[str, int],
                state_variable=beam.DoFn.StateParam(SEEN_PARTITIONS),
                *args, **kwargs):
        # Connect to Kafka, and grab the number of partitions
        topic, unused_pulse = t
        partitions = self._consumer.partitions_for_topic(topic)

        known_partitions = state_variable.read()
        # TODO: we need a timer too for handling draining
        for partition in partitions:
            if partition in known_partitions:
                continue
            else:
                state_variable.add(partition)
                yield topic, partition

    def teardown(self):
        del self._consumer


class ConsumerDoFn(beam.DoFn, RestrictionProvider):
    def __init__(self, bootstrap: List[str], poll_interval: int, group_id: str):
        self._bootstrap = bootstrap
        self._poll_interval = poll_interval
        self._group_id = group_id
        self._consumer: Optional[KafkaConsumer] = None  # data
        self._peeker: Optional[KafkaConsumer] = None  # metadata

        self._last_offset = {}

    def setup(self):
        self._consumer = KafkaConsumer(group_id=self._group_id,
                                       bootstrap_servers=self._bootstrap,
                                       enable_auto_commit=False)

        self._peeker = KafkaConsumer(group_id="metadata-peeker",
                                       bootstrap_servers=self._bootstrap,
                                       enable_auto_commit=False)

    @beam.DoFn.unbounded_per_element()
    def process(self,
                t: Tuple[str, int],
                tracker=beam.DoFn.RestrictionParam(),
                watermark_estimator=beam.DoFn.WatermarkEstimatorParam(MonotonicWatermarkEstimator.default_provider()),
                *args, **kwargs):
        topic, partition = t
        tp = TopicPartition(topic=topic, partition=partition)
        self._consumer.assign([tp])
        w = watermark_estimator.watermark_estimator_provider

        while True:
            batch = self._consumer.poll(timeout_ms=self._poll_interval*1000)
            if batch:
                values = batch[tp]

                for v in values:
                    offset = v.offset
                    if tracker.try_claim(offset):
                        self._last_offset[tp] = offset
                        #w.observe_timestamp(datetime.datetime.now())  # processing time
                        current_watermark = w.current_watermark()
                        if v.timestamp > current_watermark:
                            current_watermark = v.timestamp
                        w.observe_timestamp(current_watermark)
                        yield v
                    else:
                        self._consumer.commit(self._last_offset)
                        return

                self._consumer.commit(self._last_offset)

    def teardown(self):
        del self._consumer
        del self._peeker

    ## RestrictionProvider methods
    def create_tracker(self, restriction: OffsetRange):
        tracker = OffsetRestrictionTracker(restriction)
        return UnsplittableRestrictionTracker(underling_tracker=tracker)

    def initial_restriction(self, unused_element: Tuple[str, int]) -> OffsetRange:
        big_number = sys.maxsize
        return OffsetRange(0, big_number)

    def restriction_size(self, element: Tuple[str, int], restriction: OffsetRange):
        topic, partition = element

        tp = TopicPartition(topic=topic, partition=partition)
        r = self._peeker.end_offsets([tp])
        last_offset = r[tp]

        size = last_offset - self._last_offset
        return size
