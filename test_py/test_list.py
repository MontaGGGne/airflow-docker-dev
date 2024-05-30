a1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
a2 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
a3 = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
n1 = 3
# n2 = 3
# n3 = 4
b_1 = a1[::n1]
b_2 = a2[::n1]
b_3 = a3[::n1]
print(b_1)
print(b_2)
print(b_3)

print(len(b_3))
for index, i in enumerate(b_3):
    if index + 4 > len(a3):
        print('Break!!!')
        break
    print(f"{i}\n")

prod_num = 1
prod_num_2 = 2
prod_num_3 = 3

buffer_data_id_1: int = 0
buffer_data_id_2: int = 0
buffer_data_id_3: int = 0

for data_id, data_line in enumerate(a1):
    if buffer_data_id_1+3 != data_id:
        continue
    buffer_data_id_1 = data_id
    print(data_line)
print('#########################################')

for data_id, data_line in enumerate(a2):
    if buffer_data_id_2+3 != data_id:
        continue
    buffer_data_id_2 = data_id
    print(data_line)
print('#########################################')

for data_id, data_line in enumerate(a3):
    if buffer_data_id_3+3 != data_id:
        continue
    buffer_data_id_3 = data_id
    print(data_line)






def wdfvvwrff():
    path = hjkedfbvihebfr()


    input