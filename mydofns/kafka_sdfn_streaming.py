import logging
import sys
import time
import typing
from typing import Optional

import apache_beam as beam
from apache_beam import RestrictionProvider
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.consumer.fetcher import ConsumerRecord

from mydofns.synthetic_sdfn_streaming import MyPartitionRestrictionTracker


class ReadPartitionsDoFn(beam.DoFn):
    def __init__(self, topic: str, bootstrap_server: str, *unused_args, **unused_kwargs):
        self._topic = topic
        self._bootstrap = bootstrap_server
        self._kafka_client: Optional[KafkaConsumer] = None
        super().__init__(*unused_args, **unused_kwargs)

    def setup(self):
        self._kafka_client = KafkaConsumer(self._topic,
                                           group_id='get-partitions-group',
                                           bootstrap_servers=[self._bootstrap])

    def process(self, element, *args, **kwargs):
        partitions = self._kafka_client.partitions_for_topic(self._topic)
        for partition in partitions:
            yield partition


class ProcessKafkaPartitionsDoFn(beam.DoFn, RestrictionProvider):
    POLL_TIMEOUT = 0.1

    def __init__(self, topic: str, bootstrap_server: str, *unused_args, **unused_kwargs):
        self._topic = topic
        self._bootstrap = bootstrap_server
        self._kafka_client: Optional[KafkaConsumer] = None
        super().__init__(*unused_args, **unused_kwargs)

    def _create_consumer(self, partition):
        tp = TopicPartition(topic=self._topic, partition=partition)
        self._kafka_client = KafkaConsumer(group_id='my-beam-consumer-group',
                                           bootstrap_servers=[self._bootstrap],
                                           auto_offset_reset='earliest',
                                           enable_auto_commit=False)
        self._kafka_client.assign([tp])

    @beam.DoFn.unbounded_per_element()
    def process(self,
                partition: int,
                tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(),
                wm_estim=beam.DoFn.WatermarkEstimatorParam(WalltimeWatermarkEstimator.default_provider()),
                **unused_kwargs) -> typing.Iterable[str]:
        if self._kafka_client is None:
            self._create_consumer(partition)

        tp = TopicPartition(topic=self._topic, partition=partition)

        while True:
            offset_to_process: typing.Dict = self._kafka_client.poll()
            last_offset = self._kafka_client.end_offsets([tp])[tp]
            last_seen_offset = self._kafka_client.committed(tp)
            if offset_to_process != {}:
                all_records: typing.List[ConsumerRecord] = offset_to_process[tp]

                for record in all_records:
                    offset = record.offset
                    if tracker.try_claim(offset):
                        msg = f"Partition: {partition}, offset: {offset}   Last: {last_offset}"
                        yield msg
                        offset_metadata = OffsetAndMetadata(offset, "")
                        self._kafka_client.commit({tp: offset_metadata})
                    else:
                        return
            else:
                logging.info(f" ** Partition {partition}: Empty (offset last: {last_offset}, initial: {last_seen_offset})")
                time.sleep(self.POLL_TIMEOUT)

    def create_tracker(self, restriction: OffsetRange) -> RestrictionTracker:
        return MyPartitionRestrictionTracker(restriction)

    def initial_restriction(self, partition: int) -> OffsetRange:
        if self._kafka_client is None:
            self._create_consumer(partition)

        committed_offset = self._kafka_client.committed(TopicPartition(topic=self._topic, partition=partition))
        if committed_offset is None:
            committed_offset = 0
        return OffsetRange(committed_offset, sys.maxsize)

    def restriction_size(self, element: int, restriction: OffsetRange):
        return restriction.size()
