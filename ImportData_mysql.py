#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql
from contextlib import contextmanager
import csv

@contextmanager
def get_conn(**kwargs):
    try:
        conn = pymysql.connect(host=kwargs.get('host', 'localhost'),
                               user=kwargs.get('user', 'root'),
                               password=kwargs.get('password', '123456'),
                               db=kwargs.get('db', 'test'),
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


def insert_data(conn, bk_biz_id, bk_biz_name, administrator_id, bk_sops_id, bk_sops_name,
                DevOps_group_id,
                view_group_id, create_time):
    insert_data: str = """
                        insert into biz_table 
                        (bk_biz_id,bk_biz_name,administrator_id,bk_sops_id,bk_sops_name,DevOps_group_id,view_group_id,create_time)
                        values 
                        ({0},'{1}',{2},{3},'{4}',{5},{6},'{7}');
                      """
    sql_insert_data = insert_data.format(bk_biz_id, bk_biz_name, administrator_id, bk_sops_id, bk_sops_name,
                                         DevOps_group_id, view_group_id, create_time)
    # print(sql_insert_data)
    execute_sql(conn, sql_insert_data)
    conn.commit()
    print("插入数据成功")


def main():
    conn_args = dict(host="localhost",
                     user="root",
                     password="123456",
                     db="test")
    with open('demo_data.csv') as f:
        f_csv = csv.reader(f)
        headers = next(f_csv)
        for row in f_csv:
            print(row)

            with get_conn(**conn_args) as conn:
                insert_data(conn=conn,
                            bk_biz_id=int(row[0]),
                            bk_biz_name=row[1],
                            administrator_id=int(row[2]),
                            bk_sops_id=int(row[3]),
                            bk_sops_name=row[4],
                            DevOps_group_id=int(row[5]),
                            view_group_id=int(row[6]),
                            create_time=row[7])



if __name__ == '__main__':
    main()
