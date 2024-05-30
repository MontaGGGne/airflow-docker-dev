import time
import os
from datetime import datetime



# test_time = datetime(0, 0, 0, 0, 10, 0) # 19:40 27 мая 2024 года
# test_time_sec = test_time.timestamp()
test_10_time = 600.0
loc = time.localtime(test_10_time)
time_str = time.strftime('%Y-%m-%d %H:%M:%S', loc)

start_time_1 = datetime(2024, 5, 27, 19, 40, 0) # 19:40 27 мая 2024 года
end_time_1 = datetime(2024, 5, 27, 19, 50, 0) # 19:50 27 мая 2024 года

start_time_2 = datetime(2024, 5, 27, 20, 40, 0) # 19:40 27 мая 2024 года
end_time_2 = datetime(2024, 5, 27, 20, 50, 0) # 19:50 27 мая 2024 года

start_time_3 = datetime(2024, 6, 25, 12, 0, 0) # 19:40 27 мая 2024 года
end_time_3 = datetime(2024, 6, 25, 12, 30, 0) # 19:50 27 мая 2024 года


start_time_sec_1 = start_time_1.timestamp()
end_time_sec_1 = end_time_1.timestamp()

start_time_sec_2 = start_time_2.timestamp()
end_time_sec_2 = end_time_2.timestamp()

start_time_sec_3 = start_time_3.timestamp()
end_time_sec_3 = end_time_3.timestamp()

res_1 = abs(start_time_sec_1 - end_time_sec_1)
res_2 = start_time_sec_2 - end_time_sec_2
res_3 = start_time_sec_3 - end_time_sec_3

current_time = time.time()
local_time = time.localtime(current_time)
current_time_str = time.strftime('%Y-%m-%d %H:%M:%S', local_time)
time_stamp = datetime.strptime(current_time_str, '%Y-%m-%d %H:%M:%S').timestamp()


# wwdeef = current_time.isoformat()


# for something in os.listdir("test_f"):
#     if os.path.isdir(something):
#         print(something)


current_time = time.time()
formatted_time_list = time.ctime(current_time).split(' ')

print(formatted_time_list)

local_time = time.localtime(current_time)
print(local_time)

gm_time = time.gmtime(current_time)
print(gm_time)

local_time = time.localtime(current_time)
day = local_time.tm_mday
mounth = local_time.tm_mon
year = local_time.tm_year
hours = local_time.tm_hour
minuts = local_time.tm_min
seconds = local_time.tm_sec
formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
print(formatted_time)