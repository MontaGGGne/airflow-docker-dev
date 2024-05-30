import json
import os


data_1 = {'name': 'John'}
data_2 = {'age': 30} 
data_3 = {'city': 'New York 1'}
try:
    with open('unit_number_1.json', 'r') as j:
        fcc_data = json.load(j)
        with open('unit_number_1.json', 'w') as f:
            fcc_data.update(data_2)
            json.dump(fcc_data, f)

except:
    with open('unit_number_1.json', 'w') as f:
        json.dump(data_1, f)


file_path = os.path.realpath(__file__)
 
# Получение директории, в которой находится файл скрипта
script_dir = os.path.dirname(file_path)
 
print(script_dir)