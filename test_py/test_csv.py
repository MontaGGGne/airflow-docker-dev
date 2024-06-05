import csv
import os


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

csv_var = ''
list_csv = []
with open('csv_files/test_FD001.csv', 'r') as csv_f:
    csv_var = csv_f.read()
    for i in csv_var.split('\n'):
        list_csv.append(i.split(','))

print(list_csv)
    
# print(csv_var)


# spamreader = ''
# with open('csv_files/test_FD001.csv', 'r') as csvfile:
#     spamreader = csv.reader(csvfile, delimiter=',')
#     print('wsdsawerf')

# print(spamreader)

# with open("csv_files/test_FD001.csv", 'r') as csvfile:
#     csvreader = csv.reader(csvfile)
#     for row in csvreader:
#         print(row)

num_count = 0
for num in set(unit_numbers):
    if num_count == 0:
        num_count = unit_numbers.count(num)
    elif unit_numbers.count(num) < num_count:
        num_count = unit_numbers.count(num)