#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
from contextlib import contextmanager


@contextmanager
def get_conn(**kwargs):
    try:
        conn = pymysql.connect(host=kwargs.get('host', 'localhost'),
                               user=kwargs.get('user', 'root'),
                               password=kwargs.get('password', '123456'),
                               db=kwargs.get('db'),
                               port=kwargs.get('port', 3306),
                               charset=kwargs.get('charset', 'utf8'))
        print("连接数据库成功")
        yield conn
    except pymysql.err.OperationalError as err:
        print(err)
        raise err
    finally:
        if conn:
            print("断开数据库连接")
            conn.close()


def execute_sql(conn, sql):
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except pymysql.err as _error:
            raise _error
    return cur


def create_database(conn, database_name):
    sql_create_database = f"CREATE DATABASE IF NOT EXISTS {database_name} DEFAULT CHARSET utf8 COLLATE utf8_general_ci;"
    execute_sql(conn, sql_create_database)
    print("创建数据库成功")


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
    print("创建数据库表成功")


def main():
    conn_args = dict(host="localhost",
                     user="root",
                     password="123456",
                     )
    database_name = "test03"

    with get_conn(**conn_args) as conn:
        create_database(conn, database_name=database_name)

    conn_args["db"] = database_name

    with get_conn(**conn_args) as conn:
        create_table(conn)


if __name__ == '__main__':
    main()
