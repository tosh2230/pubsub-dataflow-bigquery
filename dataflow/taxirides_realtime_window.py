import argparse
import json
from logging import INFO, getLogger

from apache_beam import DoFn, ParDo, Pipeline, WindowInto
from apache_beam.io import BigQueryDisposition, ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Count
from apache_beam.transforms.window import FixedWindows

logger = getLogger()
logger.setLevel(INFO)


def create_count_pair(element):
    data = json.loads(element.data.decode("utf-8"))
    return data["ride_id"], 1


def main(
    input_subscription: str,
    beam_args: list[str] = None,
) -> None:

    options = PipelineOptions(
        beam_args,
        save_main_session=True,
        streaming=True,
    )
    pipeline = Pipeline(options=options)
    count = (
        pipeline
        | "Read from Pub/Sub"
        >> ReadFromPubSub(
            subscription=input_subscription, with_attributes=True, id_label="message_id"
        )
        | WindowInto(FixedWindows(120))
        | Count.PerKey()
    )

    pipeline.run()


def parse_args() -> argparse.Namespace | list[str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument("--output_dataset", help="Output BigQuery dataset.")
    parser.add_argument("--output_table", help="Output BigQuery table for results.")
    parser.add_argument(
        "--output_error_table", help="Output BigQuery table for errors."
    )
    return parser.parse_known_args()


if __name__ == "__main__":
    args, beam_args = parse_args()
    main(
        input_subscription=args.input_subscription,
        beam_args=beam_args,
    )
