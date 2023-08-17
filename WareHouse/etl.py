import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# 此代码旨在将数据加载到暂存表中，然后将这些暂存表中的数据插入 Amazon Redshift 数据库中的其他表中。


# 此函数迭代“copy_table_queries”列表中的每个查询。
def load_staging_tables(cur, conn):
    # 每个查询都使用游标（`cur`）执行，然后事务提交到数据库
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# 此函数迭代“insert_table_queries”列表中的每个查询。
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # `configparser`：处理配置文件的模块。
    # 使用“configparser”从“dwh.cfg”文件中读取配置。
    config = configparser.ConfigParser()
    # 使用“dwh.cfg”文件中提供的连接详细信息建立与 Redshift 集群的连接。
    config.read('dwh.cfg')
    # `psycopg2`：Python 的 PostgreSQL 适配器，允许 Python 连接到 PostgreSQL 数据库。
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # 调用“load_staging_tables”函数将数据加载到临时表中。
    load_staging_tables(cur, conn)
    # 调用“insert_tables”函数将数据从临时表插入到其他表中。
    insert_tables(cur, conn)
    
    # 关闭与 Redshift 集群的连接。
    conn.close()


if __name__ == "__main__":
    main()