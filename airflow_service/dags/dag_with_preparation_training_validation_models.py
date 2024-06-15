import boto3
import time
import os
import json
import logging
import traceback
import shutil
from airflow.decorators import dag
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from datetime import date
from prepData.prepData import PrepData
from train.train import Autoencoder_Model
from typing import Dict

YESTURDEY = date.today() - timedelta(days=1)


default_args = {
    'owner': 'airflow',
    # 'start_date': datetime.today() - timedelta(days=1), # datetime(2024, 6, 4)
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

@dag(
    dag_id='dag_with_preparation_training_validation_models',
    default_args=default_args,
    start_date=datetime(YESTURDEY.year, YESTURDEY.month, YESTURDEY.day), # datetime(2024, 6, 2), # datetime.today() - timedelta(days=1),
    schedule_interval="@daily" # timedelta(days=1)
    )
def example_dag():

    @task(task_id="put_jsons_from_s3_to_local")
    def get_jsons_from_s3_to_local(**kwargs):
        DATA_WINDOW = 3
        DATE_TIME_TEST = datetime(2024, 6, 4)

        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        USE_DIR = os.path.join(os.path.split(CURRENT_DIR)[0], 'jsons')
        AWS_ACCESS_KEY_ID = 'YCAJELpBT8X-vIXGQlDICwbEP'
        AWS_SECRET_ACCESS_KEY = 'YCNb7r_yUdhEOpHGQEW4xrUSv9_8TGjiFEr447fb'

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

        # s3 = kwargs["task_instance"].xcom_pull(task_ids="s3_connection", key="s3")

        if not os.path.isdir(USE_DIR):
            os.mkdir(USE_DIR)
        else:
            shutil.rmtree(USE_DIR)
            os.mkdir(USE_DIR)

        try:
            # cur_time: datetime = kwargs['logical_date'] # ds
            cur_time = DATE_TIME_TEST
            logging.info(f"CURRENT TIME: {cur_time}")
        except:
            cur_time: datetime = kwargs['ds'] # ds
            logging.info(f"CURRENT TIME: {cur_time}")

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
            up_time_skip += timedelta(days=1).total_seconds()

        create_date_dir: str = ''
        for index, date_id in enumerate(last_time_window[::-1]):
            date_prefix = f"units/{date_id}/"
            try:
                s3_obj = s3.list_objects_v2(Bucket='nasa-turbofans', Prefix=date_prefix, Delimiter = "/", MaxKeys=1000)
            except Exception as e:
                logging.error(traceback.format_exc())
                raise
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
        try:
            if not os.path.isdir(date_dir_path):
                os.mkdir(date_dir_path)
        except Exception as e:
            logging.error(f"ERROR: make date_dir_path - {traceback.format_exc()}")
            raise

        for unit_prefix in all_units_prefixes:
            current_unit = os.path.split(unit_prefix['Prefix'].rstrip('/'))[-1]

            current_unit_dir = os.path.join(date_dir_path, current_unit)
            try:
                if not os.path.isdir(current_unit_dir):
                    os.mkdir(current_unit_dir)
            except Exception as e:
                logging.error(f"ERROR: make current_unit_dir - {traceback.format_exc()}")
                raise

            try:
                get_all_one_unit_jsons = s3.list_objects_v2(Bucket='nasa-turbofans', Prefix=unit_prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
            except Exception as e:
                logging.error(f"ERROR: all_one_unit_jsons - {traceback.format_exc()}")
                raise
            if 'Contents' in get_all_one_unit_jsons:
                for unit_json in get_all_one_unit_jsons['Contents']:
                    try:
                        get_json_response = s3.get_object(Bucket='nasa-turbofans', Key=unit_json['Key'])
                    except Exception as e:
                        logging.error(f"ERROR: get_json_response - {traceback.format_exc()}")
                        raise
                    json_name = os.path.split(unit_json['Key'].rstrip('/'))[-1]
                    json_dir = os.path.join(current_unit_dir, json_name)

                    json_obj = json.loads(get_json_response['Body'].read())
                    with open(json_dir, 'w') as json_write:
                        json_obj = json.dump(json_obj, json_write)
        logging.info(f"Path to date dir with units: {date_dir_path}")
        return date_dir_path

    @task(multiple_outputs=True, task_id="organization_of_preprocessing_data")
    def preprocess_data(date_dir_path) -> Dict[str, str]:
        current_dir = os.path.dirname(os.path.realpath(__file__))

        name_file_train="Train.csv",
        name_file_test="Test.csv"

        processed_path_dir = os.path.join(os.path.split(current_dir)[0], 'processed')
        try:
            if not os.path.isdir(processed_path_dir):
                os.mkdir(processed_path_dir)
        except Exception as e:
            logging.error(f"ERROR: processed_path_dir - {traceback.format_exc()}")
            raise
        final_path_dir = os.path.join(os.path.split(current_dir)[0], 'final')
        try:
            if not os.path.isdir(final_path_dir):
                os.mkdir(final_path_dir)
        except Exception as e:
            logging.error(F"ERROR: final_path_dir - {traceback.format_exc()}")
            raise
        try:
            PrepData.start_prepData_for_add_traine(path_raw=date_dir_path,
                                                   path_final=final_path_dir,
                                                   Name_file_train=name_file_train,
                                                   Name_file_test=name_file_test)
        except Exception as e:
            logging.error(F"ERROR: res - PrepData.start_prepData - {traceback.format_exc()}")
            raise 
        return {"name_file_train": name_file_train, "name_file_test": name_file_test, "final_path_dir": final_path_dir}
    
    @task(task_id="train_and_valid_data")
    def train_and_vaild_data(preprocess_data_dir_dict, **kwargs):
        DAGSHUB_USER = "Dimitriy200"
        DAGSHUB_PASS = "RamZaZ3961%"
        DAGSHUB_TOKEN = "a1482d904ec14cd6e61aa6fcc9df96278dc7c911"
        
        final_path_dir = str(preprocess_data_dir_dict["final_path_dir"])
        name_file_train = str(preprocess_data_dir_dict["name_file_train"])
        name_file_test = str(preprocess_data_dir_dict["name_file_test"])

        path_to_train = os.path.join(final_path_dir, name_file_train)
        path_to_test = os.path.join(final_path_dir, name_file_test)
        
        cur_date_time = time.time()
        loc_cur_date_time = time.localtime(cur_date_time)
        str_cur_date_time = time.strftime('%Y-%m-%d_%H-%M-%S', loc_cur_date_time)

        autoencoder = Autoencoder_Model()

        res_autoencoder = autoencoder.add_train(path_to_train=path_to_train,
                                                path_to_valid=path_to_test,
                                                dagshub_toc_username=DAGSHUB_USER,
                                                dagshub_toc_pass=DAGSHUB_PASS,
                                                dagshub_toc_tocen=DAGSHUB_TOKEN)
        logging.info(f"res_autoencoder - {res_autoencoder}")
    
    jsons_dir_path = get_jsons_from_s3_to_local()

    preprocess_data_dir = preprocess_data(jsons_dir_path)

    train_and_vaild_data(preprocess_data_dir)

greet_dag = example_dag()