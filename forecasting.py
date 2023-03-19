# import logging
# import threading
# import subprocess
# import multiprocessing

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    weather = YandexWeatherAPI()
    # 1. Получите информацию о погодных условиях для указанного списка городов, используя API Яндекс Погоды.
    cities_weather_response = DataFetchingTask.get_response(weather.get_forecasting, CITIES.keys())

    # 2. Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов.
    parsed_response = DataCalculationTask().pool_process_calculation(cities_weather_response)
    parsed_response = [row for nested in parsed_response for row in nested]
    # --? Как обернуть DataCalculationTask._parse_response в генератор, чтобы возвращал по словарю, а не лист словарей?
    # --? pool_process_calculation не нашел как работать с генератором через него.

    # 3. Объедините полученные данные и сохраните результат в текстовом файле.
    agg_task = DataAggregationTask(parsed_response)
    df_aggregated = agg_task.get_aggregated_data()
    agg_task._save_to_csv()

    # 4. Проанализируйте результат и сделайте вывод, какой из городов наиболее благоприятен для поездки.
    analyzed_task = DataAnalyzingTask(df_aggregated)
    return analyzed_task.get_analyzed_weather_data()

    # На тесты времени не осталось, сделаю со следующими комментариями


if __name__ == "__main__":
    forecast_weather()
