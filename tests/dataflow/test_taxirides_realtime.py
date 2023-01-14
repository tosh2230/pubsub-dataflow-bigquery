import json
from datetime import datetime
from typing import Any

import pytest
from apache_beam.io import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue, assert_that, equal_to
from apache_beam.transforms.core import Create
from apache_beam.transforms.window import GlobalWindow, TimestampedValue

from dataflow.taxirides_realtime import ParseMessages

ARRIVED_TIMESTAMP = 60


def create_pubsub_data(
    data: list[dict[str, Any]],
    attributes: dict[str, str],
    message_ids: list[str],
    publish_times: list[datetime],
) -> list[PubsubMessage]:
    return [
        TimestampedValue(
            value=PubsubMessage(
                data=json.dumps(element).encode(),
                attributes=attributes,
                message_id=message_id,
                publish_time=publish_time,
            ),
            timestamp=ARRIVED_TIMESTAMP,
        )
        for element, message_id, publish_time in zip(data, message_ids, publish_times)
    ]


@pytest.mark.parametrize(
    (
        "message_ids",
        "publish_times",
        "data",
        "attributes",
        "valid_expected",
        "invalid_expected",
    ),
    [
        (
            [
                "000000000000001",
                "000000000000002",
                "000000000000003",
            ],
            [
                "2023-01-11 00:00:00.000000 UTC",
                "2023-01-11 00:00:01.000000 UTC",
                "2023-01-11 00:00:02.000000 UTC",
            ],
            [
                {
                    "ride_id": "test_01",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                {
                    "ride_id": "test_02",
                    "point_idx": 2,
                    "latitude": 2,
                    "longitude": 2,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                {
                    "ride_id": "test_03",
                    "point_idx": 3,
                    "latitude": 3,
                    "longitude": 3,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
            ],
            {"attributes_key": "attributes_value"},
            [
                TestWindowedValue(
                    value={
                        "ride_id": "test_01",
                        "point_idx": 1,
                        "latitude": 1,
                        "longitude": 1,
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                ),
                TestWindowedValue(
                    value={
                        "ride_id": "test_02",
                        "point_idx": 2,
                        "latitude": 2,
                        "longitude": 2,
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                ),
                TestWindowedValue(
                    value={
                        "ride_id": "test_03",
                        "point_idx": 3,
                        "latitude": 3,
                        "longitude": 3,
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                ),
            ],
            [],
        ),
        (
            [
                "000000000000004",
                "000000000000005",
                "000000000000006",
            ],
            [
                "2023-01-11 00:00:03.000000 UTC",
                "2023-01-11 00:00:04.000000 UTC",
                "2023-01-11 00:00:05.000000 UTC",
            ],
            [
                {
                    "ride_id": "test_04",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                {
                    "ride_id": "test_05",
                    "point_idx": 2,
                    "latitude": 2,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
                {
                    "ride_id": "test_06",
                    "point_idx": 3,
                    "longitude": 3,
                    "test_01": 1,
                    "test_02": 2,
                    "test_03": 3,
                },
            ],
            {"attributes_key": "attributes_value"},
            [
                TestWindowedValue(
                    value={
                        "ride_id": "test_04",
                        "point_idx": 1,
                        "latitude": 1,
                        "longitude": 1,
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                )
            ],
            [
                TestWindowedValue(
                    value={
                        "data": json.dumps(
                            {
                                "ride_id": "test_05",
                                "point_idx": 2,
                                "latitude": 2,
                                "test_01": 1,
                                "test_02": 2,
                                "test_03": 3,
                            }
                        ),
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                ),
                TestWindowedValue(
                    value={
                        "data": json.dumps(
                            {
                                "ride_id": "test_06",
                                "point_idx": 3,
                                "longitude": 3,
                                "test_01": 1,
                                "test_02": 2,
                                "test_03": 3,
                            }
                        ),
                    },
                    timestamp=ARRIVED_TIMESTAMP,
                    windows=[GlobalWindow()],
                ),
            ],
        ),
    ],
)
class TestParseMessage:
    def test_valid(
        self,
        message_ids: list[str],
        publish_times: list[datetime],
        data: list[PubsubMessage],
        attributes: dict[str, Any],
        valid_expected: list[dict[str, Any]],
        invalid_expected: list[dict[str, Any]],
    ):
        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True
        standard_options.runner = "DirectRunner"
        with TestPipeline(options=options) as p:
            valid_actual, _ = (
                p
                | Create(
                    create_pubsub_data(
                        data=data,
                        attributes=attributes,
                        message_ids=message_ids,
                        publish_times=publish_times,
                    )
                )
                | ParseMessages()
            )

            assert_that(
                actual=valid_actual,
                matcher=equal_to(expected=valid_expected),
                reify_windows=True,
            )

    def test_invalid(
        self,
        message_ids: list[str],
        publish_times: list[datetime],
        data: list[PubsubMessage],
        attributes: dict[str, Any],
        valid_expected: list[dict[str, Any]],
        invalid_expected: list[dict[str, Any]],
    ):
        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True
        with TestPipeline() as p:
            _, invalid_actual = (
                p
                | Create(
                    create_pubsub_data(
                        data=data,
                        attributes=attributes,
                        message_ids=message_ids,
                        publish_times=publish_times,
                    )
                )
                | ParseMessages()
            )

            assert_that(
                actual=invalid_actual,
                matcher=equal_to(expected=invalid_expected),
                reify_windows=True,
            )
