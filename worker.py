import boto3
# 等待一段时间再继续检查查询状态
import time
import random

# =======================需要修改===================================
# 创建Redshift客户端
client = boto3.client('redshift-data', region_name='ap-east-1')

# 您的Redshift集群的集群标识符
cluster_identifier = 'redshift-cluster-1'
# =================================================================

# 要执行的SQL查询
base_sql = """
select querytxt from stl_query
where label = 'default'
order by starttime desc;
"""

SKIP_STR = [item.lower() for item in [
    'padb_fetch_sample',
    'FROM STL_QUERY',
    'from SYS_QUERY_HISTORY']
            ]


def query(querys, wait_result=False):
    if wait_result:
        query_cache = dict()
    else:
        wait_result = None

    for sql_query in querys:
        response = client.execute_statement(
            ClusterIdentifier=cluster_identifier,
            Database='dev',  # 指定您的数据库名称
            DbUser='demo',  # 指定数据库用户
            Sql=sql_query
        )
        print(f"begin to run:\n {sql_query}")
        if wait_result:
            query_cache[sql_query] = response['Id']

    if not wait_result:
        return None

    counter = len(query_cache)
    stat = {key: False for key in query_cache}
    while counter > 0:
        for query in query_cache:
            if stat[query]:
                continue

            query_id = query_cache[query]
            response = client.describe_statement(Id=query_id)
            status = response['Status']

            if status == 'FINISHED':
                stat[query] = True
                print(f"{query_id} Query execution FINISHED.")
                counter -= 1
            elif status == 'FAILED':
                print("Query execution failed.")
                print(response)
                stat[query] = True
                counter -= 1
            else:
                print("Query is still running. Status: {}".format(status))
        time.sleep(0.1)

    del stat

    result = dict()
    for query in query_cache:
        query_id = query_cache[query]
        print(query_id)
        result = client.get_statement_result(Id=query_id)
        column_metadata = result['ColumnMetadata']
        data = result['Records']
        column_length = len(column_metadata)

        # 打印查询结果
        print("Query Results:")
        records = list()
        for row in data:
            record = [None] * column_length
            for i, value in enumerate(row):
                for k in value:
                    record[i] = value[k]
            records.append(record)
        result[query] = records

    return result


def check_query(query_str):
    query_text = query_str.strip().lower()
    if not query_text.startswith('select'):
        return False
    
    for skip in SKIP_STR:
        if query_text.find(skip) >= 0:
            return False
    return True

def extract_query_patten(query_str:str):
    # 测试了sqlparse效果不佳
    # 后续可能用多个正则来进行=值的替换
    # 或者redshift中本身就有编译后的语句
    return query_str

def warm_query(query_str_list):
    query(query_str_list)

def main():
    result = query([base_sql], wait_result=True)
    warm_query_cache = dict()
    if result:
        for item in result[base_sql]:
            query_text = item[0].strip().lower()
            if not check_query(query_text):
                continue
            
            query_patten = extract_query_patten(query_text)
            warm_query_cache[query_patten] = True

    warm_query(warm_query_cache.keys())

main()

