from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import numpy as np
import psycopg2
import json
import os
from datetime import datetime


def generate_json_data(**kwargs):
    # --- 1. Работа с метаданными (Postgres) ---
    conn = psycopg2.connect(f"host=postgres dbname=airflow user=airflow password={os.getenv('POSTGRES_PASSWORD')}")
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM ml_metadata WHERE key = 'last_y'")
        y_prev = cur.fetchone()[0]

    # --- 2. Генерация (твоя новая логика) ---
    exec_date = kwargs.get('logical_date', datetime.now())
    # Генерируем, скажем, 60 записей (по одной на каждую секунду или минуту интервала)
    data = []
    current_y = y_prev
    
    for i in range(60):
        t_abs = int(exec_date.timestamp()) + i
        hour = (exec_date.hour) % 24
        
        # Математика дрифта
        drift_factor = np.sin(2 * np.pi * t_abs / 10000)
        llambda = max(50 + 20 * np.sin(2 * np.pi * hour / 24), 5)
        requests = np.random.poisson(llambda)
        size = np.random.normal(100, 10)
        
        # Модель
        y = 0.5 * current_y + (0.015 + 0.01 * drift_factor) * (requests**1.2) + 0.15 * size + np.random.normal(0, 5)
        
        data.append({
            "timestamp": exec_date.isoformat(),
            "t": t_abs,
            "hour": hour,
            "requests": int(requests),
            "size": float(size),
            "y": float(y),
            "y_lag1": float(current_y),
            "drift_factor": float(drift_factor)
        })
        current_y = y

    # --- 3. Сохранение файла ---
    df = pd.DataFrame(data)
    file_name = f"data_{exec_date.strftime('%Y%m%d_%H%M%S')}.json"
    local_path = f"/tmp/{file_name}"
    df.to_json(local_path, orient='records', lines=True)

    # --- 4. Обновление состояния в БД ---
    with conn.cursor() as cur:
        cur.execute("UPDATE ml_metadata SET value = %s, updated_at = NOW() WHERE key = 'last_y'", (float(current_y),))
    conn.commit()
    conn.close()

    # Передаем путь в BashOperator через XCom
    kwargs['ti'].xcom_push(key='file_info', value={'local': local_path, 'name': file_name})


with DAG(
    dag_id="stream_generator_drift",
    start_date=datetime(2026, 3, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
) as dag:
    init_db = PostgresOperator(
        task_id="init_metadata_table",
        postgres_conn_id="postgres_default", # Убедись, что в Airflow создан этот коннект
        sql="""
        CREATE TABLE IF NOT EXISTS ml_metadata (
            key VARCHAR PRIMARY KEY,
            value FLOAT,
            updated_at TIMESTAMP
        );
        INSERT INTO ml_metadata (key, value, updated_at) 
        VALUES ('last_y', 0.0, NOW()) 
        ON CONFLICT (key) DO NOTHING;
        """
    )
    generate = PythonOperator(
        task_id="gen_json",
        python_callable=generate_json_data,
    )
    upload = BashOperator(
        task_id="upload_to_hdfs",
        bash_command="""
        L_PATH="{{ ti.xcom_pull(key='file_info')['local'] }}"
        F_NAME="{{ ti.xcom_pull(key='file_info')['name'] }}"
        hdfs dfs -put -f $L_PATH /user/airflow/success/$F_NAME && rm $L_PATH
        """
    )
    init_db >> generate >> upload
