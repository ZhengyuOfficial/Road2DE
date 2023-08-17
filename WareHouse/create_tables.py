import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# 此代码旨在通过在 Amazon Redshift 数据库中创建和删除表来设置数据库环境。

# 此函数迭代“drop_table_queries”列表中的每个查询。
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# 此函数迭代“create_table_queries”列表中的每个查询。
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # 使用“configparser”从“dwh.cfg”文件中读取配置。
    config = configparser.ConfigParser()
    # 使用“dwh.cfg”文件中提供的连接详细信息建立与 Redshift 集群的连接。
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # 调用“drop_tables”函数删除任何现有表（对于重置或清理数据库很有用）。
    drop_tables(cur, conn)
    # 调用“create_tables”函数在数据库中创建必要的表。
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()