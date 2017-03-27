import pymysql
from itertools import islice
from collections import Counter

conn_mysql = pymysql.connect(host='localhost', port=3306, user='root', passwd='welcome', db='dangchu')
mdb = conn_mysql.cursor()

# Handle CELL_INFO_0*.csv
# CELL_INFO_FILE = [
#     r"D:\hk\mobility\CELL_INFO_00.csv",
#     r"D:\hk\mobility\CELL_INFO_01.csv",
#     r"D:\hk\mobility\CELL_INFO_02.csv"]
CELL_INFO_FILE = [r"D:\hk\mobility\CELL_INFO_00.csv"]
CELL_OUT_FILE = r"D:\hk\mobility\CELL_OUT.csv"

uniq_cell = []

for fcell in CELL_INFO_FILE:
    with open(fcell, 'r') as csvfile:
        for line in islice(csvfile, 1, None):
            line = line.strip()
            data = line.split('\t')
            if data[3] != '':
                if float(data[4]) > 90:
                    uniq_cell.append((data[3], data[4], data[5]))
                else:
                    uniq_cell.append((data[3], data[5], data[4]))

count_cell = Counter(uniq_cell)
print(len(count_cell))

with open(CELL_OUT_FILE, 'w') as fout:
    for item in count_cell:
        fout.write('{},{},{}\n'.format(item[0], item[1], item[2]))
        # fout.write('{},{},{}\n'.format(hex(int(item[0])).upper(), item[1], item[2]))
        # fout.write('{},{},{}\n'.format(str(hex(int(item[0])))[2:].upper(),item[1],item[2]))



# Handle unique_lac_cell.csv
CELL_DATA_FILE = r"D:\hk\mobility\unique_lac_cell.csv"
CELL_DATA_OUT_FILE = r"D:\hk\mobility\CELL_DATA_OUT.csv"

uniq_cell_data = []

with open(CELL_DATA_FILE, 'r') as csvfile:
    for line in islice(csvfile, 1, None):
        line = line.strip()
        data = line.split(',')
        if data[1] != '':
            uniq_cell_data.append(int(data[1],16))

count_cell_data = Counter(uniq_cell_data)
print(len(count_cell_data))


found_zero = []
found_only = []
found_double = []
found_more = []

with open(CELL_DATA_OUT_FILE, 'w') as fout:
    for item in count_cell_data:
        # cell_num = int(item, 16)
        cell_num = item
        # print(cell_num)
        sql = 'select count(*) from unique_cell_00 where cell = {}'.format(cell_num)
        mdb.execute(sql)
        count = mdb.fetchone()[0]
        if count == 0:
            found_zero.append(cell_num)
        elif count == 1:
            found_only.append(cell_num)
        elif count == 2:
            found_double.append(cell_num)
        else:
            found_more.append(cell_num)
        fout.write('{}\n'.format(cell_num))

print('found_zero: {}'.format(len(found_zero)))
print(found_zero)
print('found_only: {}'.format(len(found_only)))
print(found_only)
print('found_double: {}'.format(len(found_double)))
print(found_double)
print('found_more: {}'.format(len(found_more)))
print(found_more)
