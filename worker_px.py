import psycopg2
import time
import random
import sqlparse
import os
from datetime import datetime
import pytz

###########################需要修改的地方##############


conf1 = {
    'host':'redshift-cluster-1.csqf0eych7mz.ap-northeast-1.redshift.amazonaws.com',
    'db':'dev',
    'user':'demo',
    'password':'Demo1234',
    'port':'5439'
}

conf2 = {
    'host':'redshift-cluster-1.csqf0eych7mz.ap-northeast-1.redshift.amazonaws.com',
    'db':'sample_data_dev',
    'user':'demo',
    'password':'Demo1234',
    'port':'5439'
}

#####################################################

# 要执行的SQL查询
base_sql = """
SELECT query_text from SYS_QUERY_HISTORY 
where status= 'success'
and error_message = ''
and query_type = 'SELECT'
and compile_time > 0
and query_text not like '%SYS_%'
and query_text not like '%STL_%'
and query_text not like '%stv_%'
and query_text not like '%stl_%'
and query_text not like '-- start%'
order by start_time desc
"""

def get_conn(conf:dict):
    conn = psycopg2.connect(
        dbname=conf['db'],
        user=conf['user'],
        password=conf['password'],
        host=conf['host'],
        port=conf['port']
    )
    return conn

def query(sql:str, conf:dict):
    conn = get_conn(conf)
    # 创建一个游标对象
    cursor = conn.cursor()

    # 执行SQL查询
    cursor.execute(sql)

    # 获取查询结果
    result = cursor.fetchall()

    # 提交更改
    conn.commit()

    # 关闭游标和连接
    cursor.close()
    conn.close()
    return result

def execute(sqls:list, conf:dict):
    success = 0
    for sql in sqls:
        try:
            conn = get_conn(conf)
            # 创建一个游标对象
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
            success +=1
        except Exception as ex:
            print(f"EXECUTE ERROR:\n{sql}\n{ex}")
            continue
        finally:
            cursor.close()
            conn.close()

    print(f"成功执行{success}")

def get_curent_time():
    # 设置时区为北京时间
    beijing_tz = pytz.timezone('Asia/Shanghai')

    # 获取当前时间
    current_time_utc = datetime.utcnow()

    # 将当前时间转换为北京时间
    current_time_beijing = current_time_utc.replace(tzinfo=pytz.utc).astimezone(beijing_tz)

    # 格式化输出当前时间（带时分秒）
    formatted_time = current_time_beijing.strftime('%Y-%m-%d_%H_%M_%S')
    return formatted_time

def main():
    result = query(base_sql, conf1)
    result = [item[0] for item in result]

    if result:
        print(f"found total {len(result)} need to analyse")
        warm_query_cache, errors = sqlparse.analyse(result)
        current_time = get_curent_time()
        
        if len(errors) > 0:
            print(f"total error sql:{len(errors)}")
            with open(f'{os.getpwd()}/{current_time}_errors.txt', mode='w') as f:
                f.writelines(errors)
        
        for key in warm_query_cache:
            print(f"need to run patten:\n{key}\nsql:\n{warm_query_cache[key]}\n")
        print(f"found {len(warm_query_cache)} pattens")
        # 直接执行首个符合patten的语句，而非使用PREPARE，原因是没法准确推断参数的数据类型
        # 参考 https://docs.aws.amazon.com/redshift/latest/dg/r_PREPARE.html
        execute(warm_query_cache.values(), conf2)

main()
