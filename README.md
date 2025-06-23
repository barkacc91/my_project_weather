 Weather Data Collector

Проект для сбора почасовых данных о погоде через Open-Meteo API с последующим сохранением в PostgreSQL.  
Использует Airflow для оркестрации процессов.

Требования
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [DBeaver](https://dbeaver.io/download/) (или другой SQL-клиент)
- Git


1. Установка и настройка 
Клонируйте репозиторий
git clone https://github.com/barkacc91/my_project_weather.git
cd my_project_weather

2. Запуск контейнеров
docker-compose up -d

3. Настройка базы данных
Подключитесь к PostgreSQL в DBeaver:
Хост: localhost
База данных: airflow
Порт: 5432
Пользователь: airflow
Пароль: airflow

4.Создайте таблицу:
CREATE TABLE public.weather(
    dag_id varchar,
    sys_time timestamp, 
    "timestamp" timestamp,
    "temperature_c" float,
    "feels_like_c" float,
    "humidity_percent" float,
    "precip_probability" float,
    "wind_speed_kmh" float,
    "weather_code" int,
    weather_description varchar
);

5. Запуск Airflow DAG
-Откройте Airflow в браузере:
http://localhost:8080
-Найдите и запустите DAG: subquery_weather_v1
6. Проверка данных:
SELECT * FROM public.weather ;
