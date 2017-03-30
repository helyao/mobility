#-*- coding: UTF-8 -*-
import time
import redis
from itertools import islice

ACC_NBR_CSV = r"D:\hk\acc_nbr_mon345.csv"
OUTPUT_NET_CSV = r"D:\hk\mobility\out_net.csv"
CACHE_TABLE = 'net1'

# Redis connection
conn = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
rdb = redis.Redis(connection_pool=conn)

# Load focal user
acc_nbr_fix = []

stime = time.time()
print('Start load focal-user, when {}'.format(time.strftime('%Y-%m-%d %X', time.localtime())))

with open(ACC_NBR_CSV, 'r', encoding='utf-8') as freader:
    for line in islice(freader, 1, None):
        line = line.strip()
        acc_nbr_fix.append(line)

ftime = time.time()
print('Finish load focal-user, when {}'.format(time.strftime('%Y-%m-%d %X', time.localtime())))
print('Cost {}s, and the totol number = {}'.format((ftime-stime), len(acc_nbr_fix)))


# CSV file output:
acc_count = 0
stime = time.time()
itime = stime
print('Start create net csv, when {}'.format(time.strftime('%Y-%m-%d %X', time.localtime())))

try:
    with open(OUTPUT_NET_CSV, "w", newline='') as csvfile:
        for focal in acc_nbr_fix:
            acc_count += 1
            list_contact = rdb.hgetall('con_{}'.format(focal))
            for contactor in list_contact:
                if not rdb.sismember(CACHE_TABLE, contactor.decode('utf-8')):
                    if contactor.decode('utf-8') in acc_nbr_fix:
                        csvfile.write('{},{},{}\r\n'.format(focal, contactor.decode('utf-8'), list_contact[contactor].decode('utf-8')))
            rdb.sadd(CACHE_TABLE, focal)
            if acc_count % 5000 == 0:
                costime = time.time() - itime
                print('focal counter = {}, cost {}s'.format(acc_count, costime))
                itime = time.time()
except Exception as ex:
    print('Exception: {}'.format(ex))

ftime = time.time()
print('Finish load focal-user, when '.format(time.strftime('%Y-%m-%d %X', time.localtime())))
print('Cost {}s, and the totol number = {}'.format((ftime-stime), acc_count))
