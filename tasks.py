from typing import Callable, Iterable, List
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from datetime import datetime

import pandas as pd
from utils import CITIES
from api_client import YandexWeatherAPI
from logger import logger

logger.name = 'tasks'


class DataFetchingTask:
    @staticmethod
    def get_response(function: Callable, items: Iterable) -> List[dict]:
        logger.info('Start sending requests')
        with ThreadPoolExecutor() as pool:
            result = pool.map(function, items)
            return [response for response in result]


class DataCalculationTask:
    START_HOUR = 9
    END_HOUR = 19
    PROCESSES = 4
    CHUNKSIZE = 1

    def _unix_to_datetime(self, unixtime: int) -> str:
        return datetime.utcfromtimestamp(unixtime).strftime('%Y-%m-%d %H:%M:%S')

    def _parse_response(self, city_weather_response: dict) -> List[dict]:
        forecast = []
        city = city_weather_response['geo_object']['province']['name']
        logger.debug(f'Start parsing {city}')
        for row in city_weather_response['forecasts'][0]['hours']:
            if self.START_HOUR <= int(row['hour']) <= self.END_HOUR:
                forecast.append(dict(city=city,
                                     hour=row['hour'],
                                     hour_ts=self._unix_to_datetime(row['hour_ts']),
                                     temp=row['temp'],
                                     condition=row['condition'],
                                     is_thunder=row['is_thunder'],
                                     wind_speed=row['wind_speed'],
                                     humidity=row['humidity']
                                     ))
        logger.debug(f'End parsing {city}')
        return forecast

    def pool_process_calculation(self, city_weather_response: List[dict]) -> List[dict]:
        logger.debug('Start calculation')
        pool = Pool(processes=self.PROCESSES)
        pool_outputs = pool.map(self._parse_response, city_weather_response, chunksize=self.CHUNKSIZE)
        pool.close()
        pool.join()
        logger.debug('End calculation')
        return pool_outputs


class DataAggregationTask:

    def __init__(self, cities_weather_measurements: List[dict]) -> None:
        self.df = pd.DataFrame(cities_weather_measurements)

    def _set_date_format(self) -> None:
        self.df['day_date'] = pd.to_datetime(self.df['hour_ts'], format='%Y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d')

    def _get_without_precipitation(self) -> pd.DataFrame:
        logger.debug('Calculating without precipitation')
        return self.df.groupby(['city', 'condition', 'day_date'], as_index=False) \
            .agg(condition_hours=('condition', pd.Series.count)) \
            .query('condition == ["clear", "cloudy"]') \
            .groupby(['city', 'day_date'], as_index=False) \
            .agg(without_precipitation=('condition_hours', sum))

    def _get_avg_temp(self) -> pd.DataFrame:
        logger.debug('Calculating average temperature')
        return self.df.groupby(['city', 'day_date'], as_index=False).agg(average_temp=('temp', pd.Series.mean))

    def _save_to_csv(self):
        self.df.to_csv('analyze.csv')

    def get_aggregated_data(self):
        self._set_date_format()

        logger.debug('Joining tables with condition and temperature')
        without_precipitation = self._get_without_precipitation()
        avg_temp = self._get_avg_temp()
        self.df = pd.merge(without_precipitation, avg_temp, on=['city', 'day_date'])
        return self.df


class DataAnalyzingTask:
    def __init__(self, weather_aggregated_data: pd.DataFrame):
        self.df = weather_aggregated_data

    def _sorting_values(self) -> None:
        logger.debug('Sorting values by avg temp and precipitation')
        self.df = self.df.sort_values(by=['average_temp', 'without_precipitation'],
                                      ascending=[False, False]) \
            .reset_index(drop=True)

    def _melting_columns(self) -> None:
        logger.debug('Transforme precipitation and avg temp column to one with values')
        self.df = pd.melt(self.df,
                          id_vars=['city', 'day_date'],
                          value_vars=['without_precipitation', 'average_temp'],
                          var_name='parameter',
                          value_name='score')

    def _pivoting_data(self) -> None:
        logger.debug('Transforme date rows to columns with temp and precipitation values')
        self.df = self.df.pivot_table(index=['city', 'parameter'],
                                      columns='day_date',
                                      values='score',
                                      aggfunc='sum',
                                      fill_value=0)
        self.df = self.df.reset_index()
        self.df.index.name = ''

        self.df['rating'] = self.df['city'].rank(method='dense', ascending=True).astype(int)

    def _calculate_average_for_dates(self) -> None:
        logger.debug('Calculate average for date columns in rows')
        date_columns = self.df.columns.to_list()[2:]
        self.df['average'] = self.df[date_columns].apply(lambda x: x.mean(), axis=1)

    def _calculate_rating_cities(self) -> None:
        self.df['rating'] = self.df['city'].rank(method='dense', ascending=True).astype(int)

    def get_analyzed_weather_data(self) -> pd.DataFrame:
        self._sorting_values()
        self._melting_columns()
        self._pivoting_data()
        self._calculate_average_for_dates()
        self._calculate_rating_cities()
        self.df.columns.name = ''
        return self.df


if __name__ == '__main__':
    weather = YandexWeatherAPI()
    # 1. Получите информацию о погодных условиях для указанного списка городов, используя API Яндекс Погоды.
    cities_weather_response = DataFetchingTask.get_response(weather.get_forecasting, CITIES.keys())

    # 2. Вычислите среднюю температуру и проанализируйте информацию об осадках за указанный период для всех городов.
    parsed_response = DataCalculationTask().pool_process_calculation(cities_weather_response)
    parsed_response = [row for nested in parsed_response for row in nested]

    # 3. Объедините полученные данные и сохраните результат в текстовом файле.
    agg_task = DataAggregationTask(parsed_response)
    df_aggregated = agg_task.get_aggregated_data()
    agg_task._save_to_csv()

    # 4. Проанализируйте результат и сделайте вывод, какой из городов наиболее благоприятен для поездки.
    analyzed_task = DataAnalyzingTask(df_aggregated)
    df_analyzed = analyzed_task.get_analyzed_weather_data()

    # Не писал ранее тесты, если есть базовые примеры как нужно тестить, хотел бы посмотреть в примере.