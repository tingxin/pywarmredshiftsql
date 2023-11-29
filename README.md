# Redshift SQL 预热
redshift SQL 首次查询由于需要编译，首次查询比较慢
这里通过查询预生产系统表获取已经查询过的语句，在生产系统预热

#快速使用
1.安装依赖
```
pip3 install -r requirement.txt
```

2.修改配置
```python
# =======================需要修改===================================
# 创建Redshift客户端
client = boto3.client('redshift-data', region_name='ap-east-1')

conf1 = {
    'cluster':'redshift-cluster-1',
    'db':'dev',
    'user':'demo'
}

conf2 = {
    'cluster':'redshift-cluster-1',
    'db':'dev',
    'user':'demo'
}
# =================================================================
```
3. 运行
```
python3 worker.py
```

## 注意
1.不支持sql中包含注释，及和sql运行无关的必要内容