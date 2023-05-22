import pytest
import json

from tasks import DataCalculationTask


@pytest.mark.parametrize('unixtime, expected_result', [(123123123, '1973-11-26 00:52:03'),
                                                       (999123999, '2001-08-29 22:26:39')
                                                       ])
def test_unix_time_converter(unixtime: int, expected_result: str):
    assert DataCalculationTask()._unix_to_datetime(unixtime) == expected_result


def test_parse_response():
    with open('examples/response_moscow.json', 'r') as file:
        raw_response = json.load(file)
    with open('examples/parsed_response.json', 'r') as file:
        parsed_response = json.load(file)
    assert DataCalculationTask()._parse_response(raw_response) == parsed_response
