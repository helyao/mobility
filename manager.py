#-*- coding: UTF-8 -*-
import csv
import time
import redis
import pymysql
import itertools
import collections
import multiprocessing

# Variables
# RUN_MODE = 'init'
RUN_MODE = 'development'
PARALLEL_NUM = 4
TOTAL_SIZE = [1000, 1000, 1000] # should match with $TABLE_NAME
PAGE_SIZE = 100

# File
NAME_CSV = r"D:\hk\mobility\acc_nbr_mon345.csv"
OUTPUT_CSV = r"D:\hk\mobility\out.csv"
OUTPUT_NET_CSV = r"D:\hk\mobility\out_net.csv"

# MySQL Variables
USERNAME = 'root'
PASSWORD = 'welcome'
DB_NAME = 'dangchu'
TABLE_NAME = ['pytab', 'pytab', 'pytab']

# Flag
TIME_NOTE = True
RESTORE_ACC_NBR = False

# Redis connection
conn = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
rdb = redis.Redis(connection_pool=conn)

def test(n):
    pro_name = multiprocessing.current_process().name
    print(pro_name, 'starting')
    print('worker', n)
    time.sleep(2)
    return 'return {}'.format(n)

def do(tab, n, m):
    global rdb
    stime, ftime, duration = 0, 0, 0
    pro_name = multiprocessing.current_process().name
    print('> START: process-num = {} and range {} to {} in table {} when {}'.format(pro_name, n, m, tab, time.strftime('%Y-%m-%d %X', time.localtime())))
    # MySQL connnection
    conn_mysql = pymysql.connect(host='localhost', port=3306, user=USERNAME, passwd=PASSWORD, db=DB_NAME)
    mdb = conn_mysql.cursor()
    # Select page content in MySQL
    sql = "select serv_id, acc_nbr, etl_type_id, calling_nbr, called_nbr, lac, cell_id, month_no, " \
          "date_no, week_no, hour_no, start_time, end_time, start_min, end_min from {} where id>={} and id<={}".format(tab, n, m)
    mdb.execute(sql)
    if TIME_NOTE:
        # Record start time
        stime = time.time()
    # Task do
    #   serv_id acc_nbr etl_type_id calling_nbr called_nbr  lac cell_id month_no    date_no week_no hour_no start_time  end_time    start_min   end_min
    #   0       1       2           3           4           5   6       7           8       9       10      11          12          13          14
    for item in mdb:
        # Get timestamp by month_no, date_no, hour_no
        # strTimestamp = '{}{:0>2d}{}'.format(item[7], item[8], item[11])
        # timestamp = time.mktime(datetime.datetime.strptime(strTimestamp, '%Y%m%d%H:%M:%S').timetuple())
        # rdb.lpush('T_Interval_{}'.format(item[1]), timestamp)
        # Restore acc_nbr
        rdb.sadd('acc_nbr', item[1])
        # All records
        rdb.incr('N_Record_{}'.format(item[1]))
        # Number of contacts
        if item[1] != item[3]:
            rdb.lpush('N_Contact_{}'.format(item[1]), item[3])
        if item[1] != item[4]:
            rdb.lpush('N_Contact_{}'.format(item[1]), item[4])
        # rdb.sadd('N_Contact_{}'.format(item[1]), item[3])
        # rdb.sadd('N_Contact_{}'.format(item[1]), item[4])
        # Type = voice
        if item[2] in ["21", "31"]:
            rdb.incr('N_Call_{}'.format(item[1]))
            # Calling duration Tcall
            rdb.lpush('T_Call_{}'.format(item[1]), (item[14]-item[13]))
        # Type = data
        elif item[2] in ["22", "32"]:
            rdb.incr('N_Data_{}'.format(item[1]))
        # Type = message
        elif item[2] in ["24", "34"]:
            rdb.incr('N_SMS_{}'.format(item[1]))
        # With location info(bad sensor | landline | so-on)
        if item[6] not in ["-1", "0"]:
            # calculate q
            qtemp = '{}{:0>2d}{:0>2d}'.format(item[7], item[8], item[10])
            rdb.sadd('Q_value_{}'.format(item[1]), qtemp)
            # Record with location info
            rdb.incr('N_Record_Loc_{}'.format(item[1]))
            # Number of diff location when contact
            loctemp = '{}|{}'.format(item[10], item[11])
            rdb.sadd('N_Uniq_Loc_{}'.format(item[1]), loctemp)
        # Calculate time interval
        # try:
        #     lasttime = float(rdb.hget('timestamp', item[1]).decode('utf-8'))
        #     print('timestamp', timestamp)
        #     print('lasttime', lasttime)
        #     rdb.lpush('T_Interval_{}'.format(item[1]), (timestamp-lasttime))
        # except Exception as ex:
        #     print(ex)
        # rdb.hset('timestamp', item[1], timestamp)

    if TIME_NOTE:
        # Record finish time
        ftime = time.time()
        duration = ftime - stime
        print('@ TIME: range {} to {} in table {} cost {}s'.format(n, m, tab, duration))
    print('# END: process-num = {} and range {} to {} in table {}'.format(pro_name, n, m, tab))
    return duration

def init():
    global rdb
    # Clear Redis
    rdb.flushdb()
    if RESTORE_ACC_NBR:
        # Insert user nbr from csv file
        stime = time.time()
        with open(NAME_CSV) as f:
            lines = f.readlines()
            for line in itertools.islice(lines, 1, None):
                acc_nbr = line.strip('\n')
                rdb.sadd('acc_nbr', acc_nbr)
        ftime = time.time()
        print('@ TIME: Init Redis cost {}s'. format(ftime-stime))


if __name__ == '__main__':
    stime = time.time()
    print('MAIN START: {}'.format(stime))
    init()
    if RUN_MODE == 'production':
        pass
    elif RUN_MODE == 'development':
        result = []
        # Multi-process
        pool = multiprocessing.Pool(processes=PARALLEL_NUM)
        # Traversal $TABLE_NAME
        for num in range(len(TABLE_NAME)):
            # MySQL page number
            page_num = (TOTAL_SIZE[num] + 1) // PAGE_SIZE
            for page in range(page_num):
                start_item = page * PAGE_SIZE
                if page == (page_num -1):
                    end_item = TOTAL_SIZE[num]
                else:
                    end_item = start_item + PAGE_SIZE
                result.append(pool.apply_async(func=do, args=(TABLE_NAME[num], start_item+1, start_item+PAGE_SIZE)))
        pool.close()
        pool.join()
        print('Finish')
        # Print results
        for item in result:
            print(item.get())
        print('Total process num: {}'.format(len(result)))
        # CSV file output:
        print('CREATE CSV OUTPUT FILE')
        print('> START when {}'.format(time.strftime('%Y-%m-%d %X', time.localtime())))
        with open(OUTPUT_CSV, "w", newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['acc_nbr', 'n_record', 'n_record_loc', 'n_contact', 'n_call', 'n_data', 'n_sms', 'q_value', 'n_uniq_loc'])
            acc_nbr_list = rdb.smembers('acc_nbr')
            for acc_nbr in acc_nbr_list:
                acc_nbr = acc_nbr.decode('utf-8')
                n_record = rdb.get('N_Record_{}'.format(acc_nbr))
                n_record = n_record.decode('utf-8') if n_record else 0
                n_record_loc = rdb.get('N_Record_Loc_{}'.format(acc_nbr))
                n_record_loc = n_record_loc.decode('utf-8') if n_record_loc else 0
                list_contact = rdb.lrange('N_Contact_{}'.format(acc_nbr), 0, -1)
                count_contact = collections.Counter(list_contact)
                n_contact = len(count_contact)
                n_call = rdb.get('N_Call_{}'.format(acc_nbr))
                n_call = n_call.decode('utf-8') if n_call else 0
                n_data = rdb.get('N_Data_{}'.format(acc_nbr))
                n_data = n_data.decode('utf-8') if n_data else 0
                n_sms = rdb.get('N_SMS_{}'.format(acc_nbr))
                n_sms = n_sms.decode('utf-8') if n_sms else 0
                q_value = rdb.scard('Q_value_{}'.format(acc_nbr))
                q_value = q_value if q_value else 0
                n_uniq_loc = rdb.scard('N_Uniq_Loc_{}'.format(acc_nbr))
                n_uniq_loc = n_uniq_loc if n_uniq_loc else 0
                writer.writerow([acc_nbr, n_record, n_record_loc, n_contact, n_call, n_data, n_sms, q_value, n_uniq_loc])
        print('CSV FILE CLOSE')
        # CSV file output:
        print('CREATE NET CSV OUTPUT FILE')
        print('> START when {}'.format(time.strftime('%Y-%m-%d %X',time.localtime())))
        with open(OUTPUT_NET_CSV, "w", newline='') as csvfile:
            acc_nbr_list = rdb.smembers('acc_nbr')
            for acc_nbr in acc_nbr_list:
                acc_nbr = acc_nbr.decode('utf-8')
                # print(acc_nbr)
                list_contact = rdb.lrange('N_Contact_{}'.format(acc_nbr), 0, -1)
                count_contact = collections.Counter(list_contact)
                for contactor in count_contact:
                    # print(contactor)
                    rdb.sismember('net', contactor.decode('utf-8'))
                    csvfile.write('| {} | {} | {} |\n'.format(acc_nbr, contactor.decode('utf-8'), count_contact[contactor]))
                rdb.sadd('net', acc_nbr)
        print('NET CSV FILE CLOSE')


    elif RUN_MODE == 'test':
        pool = multiprocessing.Pool(processes=PARALLEL_NUM)
        result = []
        for i in range(9):
            result.append(pool.apply_async(func=test, args=(i,)))
        pool.close()
        pool.join()
        for item in result:
            print(item.get())
    ftime = time.time()
    print('MAIN FINISH: {}'.format(ftime))
    print('TOTOL COST: {}s'.format(ftime-stime))