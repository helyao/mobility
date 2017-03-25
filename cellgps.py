from itertools import islice
from collections import Counter

CELL_INFO_FILE = [
    r"D:\hk\mobility\CELL_INFO_00.csv",
    r"D:\hk\mobility\CELL_INFO_01.csv",
    r"D:\hk\mobility\CELL_INFO_02.csv"]

CELL_OUT_FILE = r"D:\hk\mobility\CELL_OUT.csv"

uniq_cell = []

for fcell in CELL_INFO_FILE:
    with open(fcell, 'r') as csvfile:
        for line in islice(csvfile, 1, None):
            line = line.strip()
            data = line.split('\t')
            if data[3] != '':
                uniq_cell.append((data[3], data[4], data[5]))

count_cell = Counter(uniq_cell)
print(len(count_cell))

with open(CELL_OUT_FILE, 'w') as fout:
    for item in count_cell:
        fout.write('{},{},{}\n'.format(item[0], item[1], item[2]))
        # fout.write('{},{},{}\n'.format(hex(int(item[0])).upper(), item[1], item[2]))
        # fout.write('{},{},{}\n'.format(str(hex(int(item[0])))[2:].upper(),item[1],item[2]))
