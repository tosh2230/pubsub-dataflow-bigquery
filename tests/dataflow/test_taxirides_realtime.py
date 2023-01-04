import json
from typing import Any

import pytest
from apache_beam import Create, ParDo
from apache_beam.io import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow.taxirides_realtime import INVALID_TAG, VALID_TAG, ParseMessage


def create_pubsub_data(
    data: list[dict[str, Any]], attributes: dict[str, str]
) -> list[PubsubMessage]:
    return [
        PubsubMessage(
            data=json.dumps(element).encode(),
            attributes=attributes,
        )
        for element in data
    ]


@pytest.mark.parametrize(
    ("data", "attributes", "valid_expected", "invalid_expected"),
    [
        (
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
                {
                    "ride_id": "test_01",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                },
                {
                    "ride_id": "test_02",
                    "point_idx": 2,
                    "latitude": 2,
                    "longitude": 2,
                },
                {
                    "ride_id": "test_03",
                    "point_idx": 3,
                    "latitude": 3,
                    "longitude": 3,
                },
            ],
            [],
        ),
        (
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
                {
                    "ride_id": "test_04",
                    "point_idx": 1,
                    "latitude": 1,
                    "longitude": 1,
                },
            ],
            [
                {
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
                {
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
            ],
        ),
    ],
)
class TestParseMessage:
    def test_valid(
        self,
        data: list[PubsubMessage],
        attributes: dict[str, Any],
        valid_expected: list[dict[str, Any]],
        invalid_expected: list[dict[str, Any]],
    ):
        options = PipelineOptions()
        standard_options = options.view_as(StandardOptions)
        standard_options.streaming = True
        with TestPipeline(options=options) as p:
            valid_actual, _ = (
                p
                | Create(create_pubsub_data(data=data, attributes=attributes))
                | ParDo(ParseMessage()).with_outputs(INVALID_TAG, main=VALID_TAG)
            )

            assert_that(actual=valid_actual, matcher=equal_to(expected=valid_expected))

    def test_invalid(
        self,
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
                | Create(create_pubsub_data(data=data, attributes=attributes))
                | ParDo(ParseMessage()).with_outputs(INVALID_TAG, main=VALID_TAG)
            )

            assert_that(
                actual=invalid_actual, matcher=equal_to(expected=invalid_expected)
            )
