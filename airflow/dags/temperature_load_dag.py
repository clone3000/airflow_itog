from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import os

def create_tables():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS temperature_history (
                id VARCHAR(255),
                room_id VARCHAR(255),
                noted_date TIMESTAMP,
                temp FLOAT,
                out_in VARCHAR(10),
                date_only DATE,
                temp_cleaned FLOAT,
                load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                load_type VARCHAR(50)
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS load_info (
                load_id SERIAL PRIMARY KEY,
                load_type VARCHAR(50),
                load_date DATE,
                records_loaded INTEGER,
                load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    engine.dispose()
    print("Таблицы созданы")

def load_full_data():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    data_file = '/tmp/temperature_transformed.parquet'
    
    if not os.path.exists(data_file):
        print("Файл с данными не найден")
        return
    
    df = pd.read_parquet(data_file)
    print(f"Прочитано {len(df)} записей")
    
    df['load_type'] = 'full'
    df['load_date'] = datetime.now()
    
    df.to_sql(
        'temperature_history',
        engine,
        if_exists='replace',
        index=False
    )
    
    with engine.connect() as conn:
        conn.execute(
            "INSERT INTO load_info (load_type, load_date, records_loaded) VALUES (%s, %s, %s)",
            ('full', datetime.now().date(), len(df))
        )
    
    print(f"Загружено {len(df)} записей")
    engine.dispose()

def load_incremental_data():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    data_file = '/tmp/temperature_transformed.parquet'
    
    if not os.path.exists(data_file):
        print("Файл с данными не найден")
        return
    
    df = pd.read_parquet(data_file)
    
    with engine.connect() as conn:
        result = conn.execute("SELECT MAX(date_only) FROM temperature_history")
        last_date = result.fetchone()[0]
    
    if last_date:
        df['date_only'] = pd.to_datetime(df['date_only'])
        df_new = df[df['date_only'] > pd.Timestamp(last_date)]
    else:
        df_new = df
    
    if len(df_new) == 0:
        print("Нет новых данных")
        return
    
    df_new['load_type'] = 'incremental'
    df_new['load_date'] = datetime.now()
    
    df_new.to_sql(
        'temperature_history',
        engine,
        if_exists='append',
        index=False
    )
    
    with engine.connect() as conn:
        conn.execute(
            "INSERT INTO load_info (load_type, load_date, records_loaded) VALUES (%s, %s, %s)",
            ('incremental', datetime.now().date(), len(df_new))
        )
    
    print(f"Загружено {len(df_new)} новых записей")
    engine.dispose()

def check_load():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM temperature_history")
        total = result.fetchone()[0]
        
        result = conn.execute("SELECT load_type, COUNT(*) FROM load_info GROUP BY load_type")
        loads = result.fetchall()
    
    print(f"Всего записей в БД: {total}")
    for load_type, count in loads:
        print(f"Загрузок типа '{load_type}': {count}")
    
    engine.dispose()

default_args = {
    'owner': 'student',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'temperature_load_dag',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    description='Загрузка температурных данных в БД'
) as dag:
    
    start_task = DummyOperator(task_id='start')
    
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables
    )
    
    full_load_task = PythonOperator(
        task_id='full_load',
        python_callable=load_full_data
    )
    
    incremental_load_task = PythonOperator(
        task_id='incremental_load',
        python_callable=load_incremental_data
    )
    
    check_task = PythonOperator(
        task_id='check_load',
        python_callable=check_load
    )
    
    end_task = DummyOperator(task_id='end')
    
    start_task >> create_tables_task
    create_tables_task >> full_load_task >> check_task >> end_task
    create_tables_task >> incremental_load_task >> check_task >> end_task