from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
import pandas as pd
from datetime import datetime, timedelta

# Конфигурация API
API_KEY = 'e789e5b762cf58b42e7f01f352bafb92'
BASE_URL = 'http://api.marketstack.com/v1/eod'
TICKERS = 'AAPL,MSFT,GOOGL'  # Пример тикеров

# Функция для получения данных из API
def fetch_stock_data():
    url = f"{BASE_URL}?access_key={API_KEY}&symbols={TICKERS}&limit=100"  # Ограничение на 100 записей
    response = requests.get(url)
    data = response.json()

    # Преобразование данных в DataFrame
    stock_data = pd.DataFrame(data['data'])

    hook = PostgresHook(postgres_conn_id='datapstg')
    connection = hook.get_conn()
    cursor = connection.cursor()

    for index, row in stock_data.iterrows():
        cursor.execute("""
            INSERT INTO data.stock_data (ticker, date, open, close, high, low, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (row['symbol'], row['date'], row['open'], row['close'], row['high'], row['low'], row['volume']))

    connection.commit()
    cursor.close()
    connection.close()

    return None


# Функция для проверки качества данных
def validate_data():

    hook = PostgresHook(postgres_conn_id='datapstg')
    connection = hook.get_conn()
    cursor = connection.cursor()
    postgresql_select_query = "select * from data.stock_data"
    cursor.execute(postgresql_select_query)
    results = cursor.fetchall()

    column_names = [desc[0] for desc in cursor.description]
    stock_data = pd.DataFrame(results, columns=column_names)
    cursor.close()
    connection.close()
    stock_data.head()
    print(stock_data.head())
    # Проверка на наличие значений
    assert stock_data[['ticker', 'date', 'open', 'close', 'high', 'low', 'volume']].notnull().all().all(), "Найдены пустые значения"

    # Проверка на допустимые диапазоны
    assert (stock_data[['open', 'close', 'high', 'low']] >= 0).all().all(), "Найдены отрицательные значения цен"

    # Проверка на уникальность записей
    assert stock_data[['ticker', 'date']].duplicated().sum() == 0, "Найдены дубликаты записей"

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('marketstack_dag', default_args=default_args, schedule_interval='@daily') as dag:
    
    fetch_data = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=fetch_stock_data,
    )

    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=lambda: validate_data(),
        provide_context=True,
    )


    fetch_data >> validate_data_task 