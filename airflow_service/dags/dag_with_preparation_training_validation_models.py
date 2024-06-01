import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from PrepData.PrepData import PrepData

AWS_ACCESS_KEY_ID = 'YCAJELpBT8X-vIXGQlDICwbEP'
AWS_SECRET_ACCESS_KEY = 'YCNb7r_yUdhEOpHGQEW4xrUSv9_8TGjiFEr447fb'


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 5
}


def s3_connection():
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    return s3


def get_jsons_from_s3():
    s3 = s3_connection()
    try:
        s3_list_objects = s3.list_objects_v2(Bucket='nasa-turbofans', Prefix='units/', Delimiter = "/", MaxKeys=1000)
    except:
        return
    if 'CommonPrefixes' in s3_list_objects:
        last_time_prefix = s3_list_objects['CommonPrefixes'][-1]['Prefix']
        for key in s3_obj['CommonPrefixes']:
            print(key['Prefix'])
    else:
        return



def put_jsons_in_local():
    s3_connection()
    # all_all_obj = s3_1.list_objects(Bucket='nasa-turbofans', MaxKeys=100000000)


with DAG(
    dag_id='ANNA_DAG_from_PotgreSQL_2_JSON_file',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    start_task = EmptyOperator(task_id='start_task', dag=dag)

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