import boto3
import time
import os
import json
import logging
import traceback
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag
from airflow.decorators import task
from datetime import datetime, timedelta
from PrepData.PrepData import PrepData


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 4),
    'retries': 5,
    'retry_delay': timedelta(seconds=10)
}


def _s3_connection(**context):
    AWS_ACCESS_KEY_ID = 'YCAJELpBT8X-vIXGQlDICwbEP'
    AWS_SECRET_ACCESS_KEY = 'YCNb7r_yUdhEOpHGQEW4xrUSv9_8TGjiFEr447fb'
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    context["task_instance"].xcom_push(key="s3", value=s3)


def _get_jsons_from_s3_to_local(ds, **context):
    DATA_WINDOW = 3
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    USE_DIR = os.path.join(os.path.split(CURRENT_DIR)[0], 'jsons')

    s3 = context["task_instance"].xcom_pull(task_ids="s3_connection", key="s3")

    if not os.path.isdir(USE_DIR):
        os.mkdir(USE_DIR)

    cur_time: datetime = ds # ds
    cur_time_sec = cur_time.timestamp()
    struct_cur_time = time.localtime(cur_time_sec)
    str_cur_time = time.strftime('%Y-%m-%d %H:%M:%S', struct_cur_time)

    last_time_window: list = [str_cur_time]
    up_time_skip = timedelta(days=1).total_seconds()

    all_units_prefixes = []

    for i in range(DATA_WINDOW-1):
        upper_time = cur_time_sec - up_time_skip
        str_upper_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(upper_time))
        last_time_window.append(str_upper_time)
        up_time_skip *= 2

    create_date_dir: str = ''
    for index, date_id in enumerate(last_time_window[::-1]):
        date_prefix = f"units/{date_id}/"
        s3_obj = s3.list_objects_v2(Bucket='nasa-turbofans', Prefix=date_prefix, Delimiter = "/", MaxKeys=1000)
        if 'CommonPrefixes' not in s3_obj:
            continue  
        else:
            all_units_prefixes.extend(s3_obj['CommonPrefixes'])
            only_date = date_id.split(' ')[0]
            if index == len(last_time_window) - 1:
                create_date_dir += f"{only_date}"
            else:
                create_date_dir += f"{only_date}_"

    date_dir_path = os.path.join(USE_DIR, create_date_dir)
    if not os.path.isdir(date_dir_path):
        os.mkdir(date_dir_path)

    for unit_prefix in all_units_prefixes:
        current_unit = os.path.split(unit_prefix['Prefix'].rstrip('/'))[-1]
        current_unit_dir = os.path.join(date_dir_path, current_unit)
        try:
            if not os.path.isdir(current_unit_dir):
                os.mkdir(current_unit_dir)
        except Exception as e:
            print(traceback.format_exc())
            logging.error(traceback.format_exc())
            raise
        all_one_unit_jsons = s3.list_objects_v2(Bucket='nasa-turbofans', Prefix=unit_prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
        if 'Contents' in all_one_unit_jsons:
            for unit_json in all_one_unit_jsons['Contents']:
                get_json_response = s3.get_object(Bucket='nasa-turbofans', Key=unit_json['Key'])

                json_name = os.path.split(unit_json['Key'].rstrip('/'))[-1]
                json_dir = os.path.join(current_unit_dir, json_name)

                json_obj = json.loads(get_json_response['Body'].read())
                with open(json_dir, 'w') as json_write:
                    json_obj = json.dump(json_obj, json_write)


with DAG(
    dag_id='dag_with_preparation_training_validation_models',
    default_args=default_args,
    schedule_interval="@daily" # timedelta(days=1)
) as dag:
    start_task = EmptyOperator(task_id='start_task', dag=dag)

    read_task = PythonOperator(
        task_id='s3_connection',
        provide_context=True,
        python_callable=_s3_connection,
        dag=dag
    )

    read_task = PythonOperator(
        task_id='get_jsons_from_s3_to_local',
        provide_context=True,
        python_callable=_get_jsons_from_s3_to_local,
        dag=dag
    )

    read_task = PythonOperator(
        task_id='preprocess_data',
        provide_context=True,
        python_callable=PrepData.employ_Pipline(),
        dag=dag
    )

    write_task = PythonOperator(
        task_id='write_task',
        provide_context=True,
        python_callable=write_to_JSON_function,
        dag=dag
    )

    end_task = EmptyOperator(task_id='end_task', dag=dag)

    start_task >> read_task >> write_task >> end_task