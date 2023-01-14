import json
from typing import Any

from apache_beam.io import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import TestWindowedValue, assert_that, equal_to
from apache_beam.transforms.window import IntervalWindow, Timestamp, TimestampedValue

from dataflow.taxirides_realtime_window import CountInFixedWindow


def create_pubsub_data(
    test_values: list[dict[str, Any]],
    attributes: dict[str, str],
) -> list[TimestampedValue]:
    return [
        TimestampedValue(
            value=PubsubMessage(
                data=json.dumps(test_value["data"]).encode(),
                attributes=attributes,
            ),
            timestamp=test_value["timestamp"],
        )
        for test_value in test_values
    ]


class TestParseMessageInWindow:
    def test_fixed_window(self):
        first = [
            {
                "data": {
                    "ride_id": "ride_01",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 90
            },
            {
                "data": {
                    "ride_id": "ride_02",
                    "point_idx": 2,
                    "latitude": 2,
                    "longitude": 2,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 90
            },
            {
                "data": {
                    "ride_id": "ride_02",
                    "point_idx": 2,
                    "latitude": 2,
                    "longitude": 2,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 90
            },
        ]
        first_late = [
            {
                "data": {
                    "ride_id": "ride_01",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 119
            }
        ]
        second = [
            {
                "data": {
                    "ride_id": "ride_03",
                    "point_idx": 3,
                    "latitude": 3,
                    "longitude": 3,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 240
            },
            {
                "data": {
                    "ride_id": "ride_04",
                    "point_idx": 4,
                    "latitude": 4,
                    "longitude": 4,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 240
            },
            {
                "data": {
                    "ride_id": "ride_01",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                "timestamp": 240
            },
        ]
        expected = [
            # First window considers late arrived data
            TestWindowedValue(
                value=("ride_01", 2),
                timestamp=Timestamp(119.999999),
                windows=[IntervalWindow(start=0, end=120)],
            ),
            TestWindowedValue(
                value=("ride_02", 2),
                timestamp=Timestamp(119.999999),
                windows=[IntervalWindow(start=0, end=120)],
            ),
            TestWindowedValue(
                value=("ride_01", 1),
                timestamp=Timestamp(359.999999),
                windows=[IntervalWindow(start=240, end=360)],
            ),
            TestWindowedValue(
                value=("ride_03", 1),
                timestamp=Timestamp(359.999999),
                windows=[IntervalWindow(start=240, end=360)],
            ),
            TestWindowedValue(
                value=("ride_04", 1),
                timestamp=Timestamp(359.999999),
                windows=[IntervalWindow(start=240, end=360)],
            ),
        ]

        stream = TestStream()
        stream.advance_watermark_to(new_watermark=0)
        stream.advance_processing_time(advance_by=90)
        stream.add_elements(
            elements=create_pubsub_data(
                test_values=first,
                attributes={},
            ),
        )
        stream.advance_processing_time(advance_by=180)
        stream.add_elements(
            elements=create_pubsub_data(
                test_values=first_late,
                attributes={},
            ),
        )
        stream.advance_watermark_to(new_watermark=180)
        stream.advance_processing_time(advance_by=240)
        stream.add_elements(
            elements=create_pubsub_data(
                test_values=second,
                attributes={},
            ),
        )

        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True
        standard_options.runner = "DirectRunner"
        with TestPipeline(options=options) as p:
            actual = p | stream | CountInFixedWindow()

            assert_that(
                actual=actual,
                matcher=equal_to(expected=expected),
                reify_windows=True,
            )
