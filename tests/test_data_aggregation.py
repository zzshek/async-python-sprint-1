import pytest
import json
import pandas as pd

from tasks import DataAggregationTask


def test_aggregation_data():
    with open('examples/input_data_for_agg.json', 'r') as file:
        cities_weather_measurements = json.load(file)
    result = DataAggregationTask(cities_weather_measurements).get_aggregated_data()
    expected_df = pd.read_csv('analyze.csv')
    assert result.to_dict('records')[0] == expected_df.to_dict('records')[0]
