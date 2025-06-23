"""
DAG для сбора и сохранения почасовых данных о погоде в Москве с использованием Open-Meteo API.
Данный DAG выполняет запрос к API, преобразует полученные данные в табличный формат
и сохраняет их в CSV-файл с меткой времени.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import requests  # Для выполнения HTTP-запросов к API
import pandas as pd  # Для работы с данными в табличном формате
import logging
import os
from sqlalchemy import create_engine

# Базовые аргументы DAG
default_args = {
    'owner': 'data_engineering',  # Владелец DAG
    'depends_on_past': False,  # Зависимость от предыдущих запусков
    'email_on_failure': False,  # Отправка email при ошибке
    'email_on_retry': False,  # Отправка email при повторной попытке
    'retries': 0,  # Количество повторных попыток при сбое
    'retry_delay': timedelta(minutes=5),  # Задержка между попытками
    'execution_timeout': timedelta(minutes=10),  # Максимальное время выполнения
}

# Константы для настройки DAG
MOSCOW_LATITUDE = 55.7558  # Географическая широта Москвы
MOSCOW_LONGITUDE = 37.6173  # Географическая долгота Москвы
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"  # Базовый URL API
DATA_SAVE_PATH = "/tmp/weather_data"  # Базовый путь для сохранения данных
DAG_FILE_NAME = os.path.splitext( os.path.basename(__file__))[0]


def weathercode_to_text(code: int) -> str:
    """
    Преобразует числовой код погоды в текстовое описание.
    
    Параметры:
        code (int): Код погоды из API Open-Meteo
        
    Возвращает:
        str: Текстовое описание погодных условий
    """
    weather_codes = {
        0: "Ясно",
        1: "Преимущественно ясно",
        2: "Переменная облачность",
        3: "Пасмурно",
        45: "Туман",
        48: "Туман с инеем",
        51: "Морось: слабая",
        53: "Морось: умеренная",
        55: "Морось: сильная",
        56: "Ледяная морось: слабая",
        57: "Ледяная морось: сильная",
        61: "Дождь: слабый",
        63: "Дождь: умеренный",
        65: "Дождь: сильный",
        66: "Ледяной дождь: слабый",
        67: "Ледяной дождь: сильный",
        71: "Снег: слабый",
        73: "Снег: умеренный",
        75: "Снег: сильный",
        77: "Снежные зерна",
        80: "Ливни: слабые",
        81: "Ливни: умеренные",
        82: "Ливни: сильные",
        85: "Снегопад: слабый",
        86: "Снегопад: сильный",
        95: "Гроза",
        96: "Гроза со слабым градом",
        99: "Гроза с сильным градом"
    }
    return weather_codes.get(code, "Неизвестно")
    
def fetch_weather_data(**context) -> None:
    """
    Основная функция для получения данных о погоде и их сохранения.
    
    Выполняет:
    1. Запрос к Open-Meteo API для получения почасового прогноза
    2. Преобразование JSON-ответа в структурированную таблицу
    3. Сохранение данных в CSV-файл с временной меткой
    
    Исключения:
        Exception: Возникает при ошибках HTTP-запроса или обработки данных
    """
    # Подключение к PostgreSQL
    engine = create_engine(f'postgresql://airflow:airflow@postgres:5432/airflow')
    # Параметры запроса к API
    params = {
        "latitude": MOSCOW_LATITUDE,
        "longitude": MOSCOW_LONGITUDE,
        "hourly": [
            "temperature_2m",  # Температура на высоте 2м (°C)
            "relativehumidity_2m",  # Относительная влажность (%)
            "apparent_temperature",  # Ощущаемая температура (°C)
            "precipitation_probability",  # Вероятность осадков (%)
            "windspeed_10m",  # Скорость ветра на высоте 10м (км/ч)
            "weathercode"  # Код погодных условий (см. weathercode_to_text)
        ],
        "timezone": "auto",  # Автоматическое определение часового пояса
        "forecast_days": 1  # Получить прогноз только на текущие сутки
    }

    try:
        # Выполнение HTTP GET-запроса с таймаутом 30 секунд
        response = requests.get(API_BASE_URL, params=params, timeout=30)
        logging.info (f'response {response}')
        response.raise_for_status()  # Проверка на ошибки HTTP
        
        data = response.json()  # Парсинг JSON-ответа
        logging.info(f'data {data}')
    # except requests.exceptions.RequestException as e:
        # Создание DataFrame с почасовыми данными
        hourly_data = data["hourly"]
        logging.info(f'hourly_data {hourly_data}')
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(hourly_data["time"]),
            "temperature_c": hourly_data["temperature_2m"],
            "feels_like_c": hourly_data["apparent_temperature"],
            "humidity_percent": hourly_data["relativehumidity_2m"],
            "precip_probability": hourly_data["precipitation_probability"],
            "wind_speed_kmh": hourly_data["windspeed_10m"],
            "weather_code": hourly_data["weathercode"]
        })
        
        # Добавление текстового описания погоды
        df["weather_description"] = df["weather_code"].apply(weathercode_to_text)
        
        # Генерация имени файла с текущей датой и временем
        sys_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        df["sys_time"] = sys_time
        df['dag_id'] = context['dag_run'].id

        # Сохранение в таблицу (если таблица существует - заменяем)
        df.to_sql(
            schema='public',
            name='weather',          # Название таблицы
            con=engine,            # Подключение SQLAlchemy
            if_exists='append',   # 'fail'(ошибка), 'replace'(удалить и создать), 'append'(добавить)
            index=False,           # Не сохранять индекс DataFrame
            method='multi'         # Пакетная вставка (ускоряет загрузку)
        )

    except requests.exceptions.RequestException as e:
        # Обработка ошибок сети/запроса
        error_msg = f"Ошибка при запросе к API: {str(e)}"
        logging.info(f'{error_msg}')
        raise Exception(error_msg)
    except (KeyError, ValueError) as e:
        # Обработка ошибок формата данных
        error_msg = f"Ошибка обработки данных: {str(e)}"
        logging.info(f'{error_msg}')
        raise Exception(error_msg)
    




# Определение DAG
with DAG(
    dag_id=DAG_FILE_NAME,  # Уникальный идентификатор DAG
    default_args=default_args,
    description='Сбор почасовых данных о погоде в Москве с Open-Meteo API',
    schedule_interval="0 12 * * *",  # Интервал запуска (каждые 3 часа)
    start_date=datetime(2023, 1, 1),  # Дата начала работы DAG
    catchup=False,  # Запрет на выполнение пропущенных запусков
    max_active_runs=1,  # Максимум 1 одновременный запуск
    tags=['weather', 'data_collection'],  # Теги для поиска/фильтрации
) as dag:
    
    # Определение задачи
    fetch_weather_task = PythonOperator(
        task_id='fetch_and_save_weather_data',  # Идентификатор задачи
        python_callable=fetch_weather_data,  # Вызываемая функция
        execution_timeout=timedelta(minutes=15),  # Таймаут для конкретной задачи
    )

    # Дополнительные задачи могут быть добавлены здесь
    # Например:
    # - validate_data_task
    # - upload_to_db_task
    # - send_notification_task

    # Определение порядка выполнения задач
    fetch_weather_task 