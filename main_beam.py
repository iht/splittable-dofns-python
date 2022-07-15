import argparse
import logging

from apache_beam import PCollection
from apache_beam.io.fileio import WriteToFiles
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.periodicsequence import PeriodicImpulse

from typing import List
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows

from mydofns.fns import PartitionWatcherDoFn
from mydofns.fns_restriction_partition import ReadFromKafkaCustomDoFnWithPartition

BOOTSTRAP = '10.1.0.58:32469'
# BOOTSTRAP = '192.168.64.2:32310'
TOPIC = "beam-topic"
POLL_INTERVAL = 3


def run_pipeline(topic: str, bootstrap_server: List[str], poll_interval: int, beam_options, output_file: str):
    pipeline_options: PipelineOptions = PipelineOptions(beam_options)

    with beam.Pipeline(options=pipeline_options) as p:
        # impulse = p | "Heart beat" >> PeriodicImpulse(fire_interval=poll_interval)
        # impulse | "Print 3" >> beam.Map(print)
        impulse = p | beam.Create([0])
        with_keys = impulse | "Add key" >> beam.WithKeys(lambda x: topic)
        ps: PCollection[int] = with_keys | "Check partitions" >> beam.ParDo(PartitionWatcherDoFn(bootstrap_server))

        msgs = ps | beam.Reshuffle() | "Read from Kafka" >> beam.ParDo(
            ReadFromKafkaCustomDoFnWithPartition(topic, bootstrap_server, poll_interval))

        msgs | beam.Map(lambda x: logging.info(f" ************** {x}"))
        msgs | "Convert" >> beam.Map(lambda s: s.encode('utf-8')) | "Write to Pubsub" >> beam.io.WriteToPubSub(
            topic="projects/ihr-hop-playground/topics/my-messages")
        # w = msgs | "Window for writing" >> beam.WindowInto(FixedWindows(10))
        # w | "Write to files" >> WriteToFiles(path=output_file, shards=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--bootstrap", type=str, required=False, default=BOOTSTRAP)
    parser.add_argument("--topic", type=str, required=False, default=TOPIC)
    parser.add_argument("--poll-interval", type=float, required=False, default=POLL_INTERVAL)
    parser.add_argument("--output-location", type=str, required=True)

    my_args, beam_args = parser.parse_known_args()

    run_pipeline(topic=my_args.topic,
                 bootstrap_server=my_args.bootstrap,
                 poll_interval=my_args.poll_interval,
                 beam_options=beam_args,
                 output_file=my_args.output_location)
