#-*- coding: UTF-8 -*-
import time
import pymysql

USERNAME = 'root'
PASSWORD = 'welcome'
DB_NAME = 'dangchu'
TABLE_NAME = 'pytab'
NAME_CSV = r"D:\winhye\python\Workin\WebApps\apollo\hk\acc_nbr_mon345.csv"
OUTPUT_CSV = r"D:\winhye\python\Workin\WebApps\apollo\hk\out.csv"

start = time.time()
print('start run',start)

fuser = open(NAME_CSV, "r")
users = fuser.readlines()

result = {}

for line in users:
    user = line.strip('\n')
    result[user] = {}
    result[user]["Q_value"] = set()     # q的分子数
    result[user]["N_Record_Loc"] = 0    # 具有地点信息的记录数
    result[user]["N_Record"] = 0        # 所有记录数
    result[user]["N_Call"] = 0          # 通话数
    result[user]["N_SMS"] = 0           # 短信数
    result[user]["N_Data"] = 0          # 数据数
    result[user]["N_Uniq_Loc"] = set()  # 地点数
    result[user]["N_Contact"] = set()   # 联系人数

conn = pymysql.connect(host='localhost', port=3306, user=USERNAME, passwd=PASSWORD, db=DB_NAME)
cur = conn.cursor()

sql = 'select * from '+ TABLE_NAME + ' limit 10000'
cur.execute(sql)

count = 0

start_db = time.time()
print('start select db', start_db)
print('duration', start_db - start)

# 逐条统计信息
for item in cur:
    count += 1
    print(item)
    if count%10000 == 0:    # 每10000条输出一次
        print("count:", count)

    if item[1] in result:
        # 所有通话记录
        result[item[1]]["N_Record"] += 1
        # 通话的朋友（无论是谁打给谁）
        result[item[1]]["N_Contact"].add(item[4])
        result[item[1]]["N_Contact"].add(item[7])
        if item[3] in ["21", "31"]:  # 语音
            result[item[1]]["N_Call"] += 1
        elif item[3] in ["22", "32"]:  # 数据
            result[item[1]]["N_Data"] += 1
        elif item[3] in ["24", "34"]:  # 短信
            result[item[1]]["N_SMS"] += 1
        if item[11] not in ["-1", "0"]:    # 有位置信息的数据 > 去掉座机
            # 计算q
            qtemp = '{}{:0>2d}{:0>2d}'.format(item[12], item[13], item[15])
            result[item[1]]["Q_value"].add(qtemp)
            # 有地点信息的通话记录数目
            result[item[1]]["N_Record_Loc"] += 1
            # 通话的地点
            loctemp = '{}|{}'.format(item[10],item[11])
            result[item[1]]["N_Uniq_Loc"].add(item[7])

# 将结果输出
fout = open(OUTPUT_CSV, "w")
header = "acc_nbr, n_record, n_record_loc, n_contact, n_call, n_data, n_sms, q_value, n_uniq_loc\n"
fout.write(header)

for user in result:
    if result[user]["N_Record"] > 0:
        line = "{},{},{},{},{},{},{},{},{}\n".format(
            user, result[user]["N_Record"], result[user]["N_Record_Loc"], len(result[user]["N_Contact"]),
            result[user]["N_Call"], result[user]["N_Data"], result[user]["N_SMS"],
            len(result[user]["Q_value"]), len(result[user]["N_Uniq_Loc"])
        )
        fout.write(line)

end = time.time()
print('end', end)
print('duration', end - start_db)

fuser.close()
fout.close()
