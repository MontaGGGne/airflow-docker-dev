import boto3
import json
import os
import logging
import traceback
import time
from datetime import datetime


session = boto3.session.Session()
s3_1 = session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id = 'YCAJELpBT8X-vIXGQlDICwbEP',
    aws_secret_access_key = 'YCNb7r_yUdhEOpHGQEW4xrUSv9_8TGjiFEr447fb')

# all_all_obj = s3_1.list_objects(Bucket='nasa-turbofans', MaxKeys=100000000)

# s3 = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix='unit_number_100/', Delimiter = "/", MaxKeys=1000)


# s3_2 = session.client(
#     service_name='s3',
#     endpoint_url='https://storage.yandexcloud.net',
#     aws_access_key_id = 'YCAJEoJ0E0v3-mwqhz2SW_3cZ',
#     aws_secret_access_key = 'YCM0wvQYLJ5CZQMq6i23c6ZyEarSiJvNUtBTolkD')

# session_1 = boto3.session.Session(aws_access_key_id='YCAJEoJ0E0v3-mwqhz2SW_3cZ', aws_secret_access_key='YCM0wvQYLJ5CZQMq6i23c6ZyEarSiJvNUtBTolkD')

# s3_client = session_1.client('s3')
# your_bucket = s3_client.Bucket('nasa-turbofans')

# for s3_file in your_bucket.objects.all():
#     print(s3_file.key)

# s3_paginator = s3_1.get_paginator('list_objects_v2')


# def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
#     prefix = prefix.lstrip(delimiter)
#     start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
#     for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
#         for content in page.get('Contents', ()):
#             yield content['Key']


# all_all_obj = s3_1.list_objects(Bucket='nasa-turbofans')
# all_obj = s3_1.list_objects(Bucket='nasa-turbofans')['Contents']

# my_bucket = s3_1.Bucket('nasa-turbofans')
# all_bucket_obj = my_bucket.objects.all()
# for file in all_bucket_obj:
#     print(file)

# for key in s3_1.list_objects(Bucket='nasa-turbofans')['Contents']:
#     print(key['Key'])

# for i in range(1, 101):
#     forDeletion = []
#     for j in range(0, 300):
#         forDeletion = [{'Key':f"unit_number_{i}/time_in_cycles_{j}.json"}]
#         response = s3_1.delete_objects(Bucket='nasa-turbofans', Delete={'Objects': forDeletion})
#         print(response)
# forDeletion = [{'Key':'test_quantity/unit_number_1.json'}]
# response = s3_1.delete_objects(Bucket='nasa-turbofans', Delete={'Objects': forDeletion})
# s3_1.upload_file('unit_number_1.json', 'nasa-turbofans', 'test_quantity/unit_number_1/27-5-2024_12-25/unit_number_1.json')

# for key in s3_1.list_objects(Bucket='nasa-turbofans')['Contents']:
#     print(key['Key'])

# 100 min

# s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix='units/', Delimiter = "/", MaxKeys=1000)
# if 'CommonPrefixes' in s3_obj:
#     last_prefix = s3_obj['CommonPrefixes'][-1]['Prefix'].rstrip('/')
#     # s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix='test_quantity/unit_number_1/', Delimiter = "/", MaxKeys=1000)
#     for key in s3_obj['CommonPrefixes']:
#         print(key['Prefix'])
# else:
#     print("Havent Prefixes")


############################################### airflow script ###############################################

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
USE_DIR = os.path.join(os.path.split(CURRENT_DIR)[0], 'jsons')
if not os.path.isdir(USE_DIR):
    os.mkdir(USE_DIR)


s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix='units/', Delimiter = "/", MaxKeys=1000)
if 'CommonPrefixes' in s3_obj:
    all_prefixes = s3_obj['CommonPrefixes']
    num_elements: int = 0
    if len(all_prefixes) == 1:
        num_elements = 1    
    elif len(all_prefixes) == 2:
        num_elements = 2
    else:
        num_elements = 3

    elements = s3_obj['CommonPrefixes'][-num_elements:]
    result_time_dir = ''
    all_units_prefixes = []
    for index, prefix in enumerate(elements):
        last_time_prefix = s3_obj['CommonPrefixes'][-1]['Prefix']

        units_prefixes = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=last_time_prefix, Delimiter = "/", MaxKeys=1000)
        if 'CommonPrefixes' in units_prefixes:
            all_units_prefixes.extend(units_prefixes['CommonPrefixes'])

        last_time_bad = os.path.split(last_time_prefix.rstrip('/'))[-1]
        last_time_stamp = datetime.strptime(last_time_bad, '%Y-%m-%d %H:%M:%S').timestamp()
        last_local_time = time.localtime(last_time_stamp)
        last_time_good = time.strftime('%Y-%m-%d_%H-%M-%S', last_local_time)

        if index == 0:
            result_time_dir = last_time_good
            continue
        result_time_dir += f"+{last_time_good}"

    last_time_dir = os.path.join(USE_DIR, result_time_dir)

    try:
        if not os.path.isdir(last_time_dir):
            os.mkdir(last_time_dir)
    except Exception as e:
        print(traceback.format_exc())
        logging.error(traceback.format_exc())
        raise

    for unit_prefix in all_units_prefixes:
        current_unit = os.path.split(unit_prefix['Prefix'].rstrip('/'))[-1]
        current_unit_dir = os.path.join(last_time_dir, current_unit)
        try:
            if not os.path.isdir(current_unit_dir):
                os.mkdir(current_unit_dir)
        except Exception as e:
            print(traceback.format_exc())
            logging.error(traceback.format_exc())
            raise
        all_one_unit_jsons = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=unit_prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
        if 'Contents' in all_one_unit_jsons:
            for unit_json in all_one_unit_jsons['Contents']:
                get_json_response = s3_1.get_object(Bucket='nasa-turbofans', Key=unit_json['Key'])

                json_name = os.path.split(unit_json['Key'].rstrip('/'))[-1]
                json_dir = os.path.join(current_unit_dir, json_name)

                json_obj = json.loads(get_json_response['Body'].read())
                with open(json_dir, 'w') as json_write:
                    json_obj = json.dump(json_obj, json_write)

# CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
# USE_DIR = os.path.join(os.path.split(CURRENT_DIR)[0], 'jsons')
# if not os.path.isdir(USE_DIR):
#     os.mkdir(USE_DIR)


# s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix='units/', Delimiter = "/", MaxKeys=1000)
# if 'CommonPrefixes' in s3_obj:
#     last_time_prefix = s3_obj['CommonPrefixes'][-1]['Prefix']
#     last_time_bad = os.path.split(last_time_prefix.rstrip('/'))[-1]

#     last_time_stamp = datetime.strptime(last_time_bad, '%Y-%m-%d %H:%M:%S').timestamp()
#     last_local_time = time.localtime(last_time_stamp)
#     last_time_good = time.strftime('%Y-%m-%d_%H-%M-%S', last_local_time)

#     last_time_dir = os.path.join(USE_DIR, last_time_good)
#     try:
#         if not os.path.isdir(last_time_dir):
#             os.mkdir(last_time_dir)
#     except Exception as e:
#         print(traceback.format_exc())
#         logging.error(traceback.format_exc())
#         raise
#     all_units_prefixes = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=last_time_prefix, Delimiter = "/", MaxKeys=1000)
#     if 'CommonPrefixes' in all_units_prefixes:
#         for unit_prefix in all_units_prefixes['CommonPrefixes']:
#             current_unit = os.path.split(unit_prefix['Prefix'].rstrip('/'))[-1]
#             current_unit_dir = os.path.join(last_time_dir, current_unit)
#             try:
#                 if not os.path.isdir(current_unit_dir):
#                     os.mkdir(current_unit_dir)
#             except Exception as e:
#                 print(traceback.format_exc())
#                 logging.error(traceback.format_exc())
#                 raise
#             all_one_unit_jsons = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=unit_prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
#             if 'Contents' in all_one_unit_jsons:
#                 for unit_json in all_one_unit_jsons['Contents']:
#                     get_json_response = s3_1.get_object(Bucket='nasa-turbofans', Key=unit_json['Key'])

#                     json_name = os.path.split(unit_json['Key'].rstrip('/'))[-1]
#                     json_dir = os.path.join(current_unit_dir, json_name)

#                     json_obj = json.loads(get_json_response['Body'].read())
#                     with open(json_dir, 'w') as json_write:
#                         json_obj = json.dump(json_obj, json_write)

#     print("Havent Prefixes")


# if __name__ == "__main__":
#     bucket_keys = keys('nasa-turbofans')
#     print(bucket_keys)




################## for deletion or for saw ##################

# forDeletion = []
# for i in range(1, 101):
#     s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=f"units/2024-05-27 17:12:36/unit_number_{i}/", Delimiter = "/", MaxKeys=1000)
#     if 'CommonPrefixes' in s3_obj:
#         for prefix in s3_obj['CommonPrefixes']:
#             jsons_prefix = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
#             del_dict_list = []
#             for content in jsons_prefix['Contents']:
#                 del_dict = {'Key': content['Key']}
#                 del_dict_list.append(del_dict)
#             # forDeletion.append(del_dict)
#             # print(Content['Key'])
#             response = s3_1.delete_objects(Bucket='nasa-turbofans', Delete={'Objects': del_dict_list})
#             print(response)


# forDeletion = []
# for i in range(1, 101):
#     s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=f"units/2024-05-27 17:27:54/unit_number_{i}/", Delimiter = "/", MaxKeys=1000)
#     for content in s3_obj['Contents']:
#         del_dict = [{'Key': content['Key']}]
#         # forDeletion.append(del_dict)
#         # print(Content['Key'])
#         response = s3_1.delete_objects(Bucket='nasa-turbofans', Delete={'Objects': del_dict})
#         print(response)


# response = s3_1.delete_objects(Bucket='nasa-turbofans', Delete={'Objects': forDeletion})

# for i in range(1, 101):
#     s3_obj = s3_1.list_objects_v2(Bucket='nasa-turbofans', Prefix=f"units/2024-05-27 17:37:54/unit_number_29/", Delimiter = "/", MaxKeys=1000)
#     if 'Contents' in s3_obj:
#         for Content in s3_obj['Contents']:
#             print(Content['Key'])

for key in s3_1.list_objects(Bucket='nasa-turbofans')['Contents']:

    print(key['Key'])