import responses
import pytest
import json

from api_client import YandexWeatherAPI
from utils import CITIES


@responses.activate
def test_valid_response_moscow():
    with open('examples/response_moscow.json', 'r') as file:
        valid_json_answer = json.load(file)
    responses.add(responses.GET, CITIES['MOSCOW'], json=valid_json_answer, status=200)
    weather = YandexWeatherAPI()
    response = weather.get_forecasting('MOSCOW')
    assert response['info']['tzinfo']['name'] == "Europe/Moscow"


@responses.activate
def test_exception_api():
    with pytest.raises(KeyError):
        weather = YandexWeatherAPI()
        weather.get_forecasting('MOSCOK')
