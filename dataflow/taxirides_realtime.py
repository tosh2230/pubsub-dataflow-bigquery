import argparse
import json
from logging import WARN, getLogger

from apache_beam import DoFn, ParDo, Pipeline
from apache_beam.io import BigQueryDisposition, ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import TaggedOutput

VALID_TAG = "valid"
INVALID_TAG = "invalid"


class ParseMessage(DoFn):
    def __init__(self):
        super(ParseMessage, self).__init__()

    def process(self, message):
        try:
            transformed = {}
            data = json.loads(message.data.decode("utf-8"))
            transformed["ride_id"] = data["ride_id"]
            transformed["point_idx"] = data["point_idx"]
            transformed["latitude"] = data["latitude"]
            transformed["longitude"] = data["longitude"]
            yield transformed
        except Exception:
            invalid = {}
            invalid["data"] = message.data.decode("utf-8")
            yield TaggedOutput(INVALID_TAG, invalid)


def get_schema(output_table) -> str:
    with open(f"/tmp/{output_table}") as file:
        return ",".join([line.strip() for line in file.readlines()])


def main(
    input_subscription: str,
    output_table: str,
    output_error_table: str,
    output_table_schema: str,
    output_error_table_schema: str,
    beam_args: list[str] = None,
) -> None:

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline = Pipeline(options=options)
    rows, error_rows = (
        pipeline
        | "Read from Pub/Sub"
        >> ReadFromPubSub(
            subscription=input_subscription, with_attributes=True, id_label="message_id"
        )
        | "Parse JSON messages"
        >> ParDo(ParseMessage()).with_outputs(INVALID_TAG, main=VALID_TAG)
    )

    _ = rows | "Write messages to BigQuery" >> WriteToBigQuery(
        output_table,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
        schema=output_table_schema,
        additional_bq_parameters={
            "timePartitioning": {
                "type": "DAY",
                "field": "timestamp",
            }
        },
    )

    _ = error_rows | "Write errors to BigQuery" >> WriteToBigQuery(
        output_error_table,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR,
        schema=output_error_table_schema,
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
    logger = getLogger()
    logger.setLevel(WARN)
    args, beam_args = parse_args()
    main(
        input_subscription=args.input_subscription,
        output_table=f"{args.output_dataset}.{args.output_table}",
        output_error_table=f"{args.output_dataset}.{args.output_error_table}",
        output_table_schema=get_schema(args.output_table),
        output_error_table_schema=get_schema(args.output_error_table),
        beam_args=beam_args,
    )
