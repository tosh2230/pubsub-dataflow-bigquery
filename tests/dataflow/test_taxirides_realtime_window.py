import json
from typing import Any

from apache_beam import Map, WindowInto
from apache_beam.io import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.combiners import Count
from apache_beam.transforms.window import FixedWindows
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.taxirides_realtime_window import create_count_pair


def create_pubsub_data(
    data: list[dict[str, Any]],
    attributes: dict[str, str],
) -> list[PubsubMessage]:
    return [
        PubsubMessage(
            data=json.dumps(element).encode(),
            attributes=attributes,
        )
        for element in data
    ]


class TestParseMessageInWindow:
    def test_fixed_window(self):
        first_data = [
            {
                "ride_id": "ride_01",
                "point_idx": 1,
                "latitude": 1,
                "longitude": 1,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
            {
                "ride_id": "ride_02",
                "point_idx": 2,
                "latitude": 2,
                "longitude": 2,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
            {
                "ride_id": "ride_02",
                "point_idx": 2,
                "latitude": 2,
                "longitude": 2,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
        ]
        second_data = [
            {
                "ride_id": "ride_03",
                "point_idx": 3,
                "latitude": 3,
                "longitude": 3,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
            {
                "ride_id": "ride_04",
                "point_idx": 4,
                "latitude": 4,
                "longitude": 4,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
            {
                "ride_id": "ride_01",
                "point_idx": 1,
                "latitude": 1,
                "longitude": 1,
                "test_01": 1,
                "test_02": 2,
                "test_03": 3,
            },
        ]
        expected = [
            ("ride_01", 1),
            ("ride_02", 2),
            ("ride_01", 1),
            ("ride_03", 1),
            ("ride_04", 1),
        ]

        stream = TestStream()
        stream.advance_processing_time(advance_by=120)
        stream.advance_watermark_to(new_watermark=60)
        stream.add_elements(
            elements=create_pubsub_data(
                data=first_data,
                attributes={},
            ),
            event_timestamp=90,
        )
        stream.add_elements(
            elements=create_pubsub_data(
                data=second_data,
                attributes={},
            ),
            event_timestamp=240,
        )

        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True
        with TestPipeline(options=options) as p:
            actual = (
                p
                | stream
                | Map(create_count_pair)
                | WindowInto(FixedWindows(120))
                | Count.PerKey()
            )

            assert_that(
                actual=actual,
                matcher=equal_to(expected=expected),
            )
