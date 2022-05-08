#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from contextlib import contextmanager


def get_conn(**kwargs):
    try:
        conn = pymysql.connect(host=kwargs.get('host', 'localhost'),
                               user=kwargs.get('user', 'root'),
                               password=kwargs.get('password', '123456'),
                               db=kwargs.get('db', 'pythonTest'),
                               port=kwargs.get('port', 3306),
                               charset=kwargs.get('charset', 'utf8'))
        print("连接数据库成功")
        return conn
    except pymysql.err.OperationalError as err:
        print(err)
        # raise err


def execute_sql(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
    return cur


def drop_table(conn):
    sql_drop_table = "DROP DATABASE IF EXISTS biz_table"
    execute_sql(conn, sql_drop_table)


def create_table(conn):
    sql_create_table = """
        create table if not exists biz_table(
        bk_biz_id int primary key auto_increment,
        bk_biz_name varchar(255),
        administrator_id int,
        bk_sops_id int,
        bk_sops_name varchar(255), 
        DevOps_group_id int,
        view_group_id int, 
        create_time datetime
        ) 
    """
    execute_sql(conn, sql_create_table)


def insert_data(conn, bk_biz_id, bk_biz_name, administrator_id, bk_sops_id, bk_sops_name, DevOps_group_id,
                view_group_id, create_time):
    insert_data: str = """
                        INSERT INTO biz_table 
                        ( bk_biz_id, bk_biz_name, administrator_id, bk_sops_id, bk_sops_name, DevOps_group_id, 
                        view_group_id, create_time)
                        VALUES
                        ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7});
                      """
    sql_insert_data = insert_data.format(bk_biz_id, bk_biz_name, administrator_id, bk_sops_id, bk_sops_name,
                                         DevOps_group_id,
                                         view_group_id, create_time)
    print(sql_insert_data)
    execute_sql(conn, sql_insert_data)


def query_data(conn):
    sql_query_data = "select * from biz_table"
    cur = execute_sql(conn,sql_query_data)
    raws = cur.fetchall()
    for row in raws:
        print(row)


def main():
    conn = get_conn(host="localhost",
                    user="root",
                    password="123456")
    try:
        # drop_table(conn)
        create_table(conn)
        # insert_data(conn)
        # query_data(conn)
        # pass
    finally:
        if conn:
            conn.close()

# fetchone() : 获取 SQL 语句的一条数据
# fetchmany() : 获取 SQL 语句的多条记录
# fetchall(): 获取 SQL 语句的所有数据

if __name__ == '__main__':
    main()
