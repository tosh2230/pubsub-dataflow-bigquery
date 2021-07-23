import argparse
import json
from typing import List, Union
from logging import getLogger, WARN

from apache_beam import Pipeline, Map, ParDo, DoFn
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
from apache_beam.pvalue import TaggedOutput
from apache_beam.options.pipeline_options import PipelineOptions

def get_schema(output_table) -> str:
    with open(f'/tmp/{output_table}') as file:
        return ','.join([line.strip() for line in file.readlines()])

class ParseMessage(DoFn):
    OUTPUT_ERROR_TAG = 'error'
    
    def process(self, line):
        try:
            parsed_row = json.loads(line)
            yield parsed_row
        except Exception as error:
            error_row = json.loads(line)
            yield TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)

def main(
    input_subscription: str,
    output_table: str,
    output_error_table: str,
    output_table_schema: str,
    beam_args: List[str] = None,) -> None:

    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    pipeline = Pipeline(options=options)
    rows, error_rows = (
        pipeline
        | "Read from Pub/Sub" >> ReadFromPubSub(subscription=input_subscription)
            .with_output_types(bytes)
        | "UTF-8 bytes to string" >> Map(lambda msg: msg.decode("utf-8"))
        | "Parse JSON messages" >> ParDo(ParseMessage())
            .with_outputs(ParseMessage.OUTPUT_ERROR_TAG,main='rows')
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
        schema=output_table_schema,
    )

    pipeline.run()

def parse_args() -> Union[argparse.Namespace ,List[str]]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--output_dataset",
        help="Output BigQuery dataset."
    )
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results."
    )
    parser.add_argument(
        "--output_error_table",
        help="Output BigQuery table for errors."
    )
    return parser.parse_known_args()

if __name__ == "__main__":
    logger = getLogger()
    logger.setLevel(WARN)
    args, beam_args = parse_args()
    main(
        input_subscription=args.input_subscription,
        output_table=f'{args.output_dataset}.{args.output_table}',
        output_error_table=f'{args.output_dataset}.{args.output_error_table}',
        output_table_schema=get_schema(args.output_table),
        beam_args=beam_args,
    )
