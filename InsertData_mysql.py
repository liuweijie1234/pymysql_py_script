#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pymysql


# 插入数据进mysql

def get_conn(**kwargs):
    try:
        conn = pymysql.connect(host=kwargs.get('host', 'localhost'),
                               user=kwargs.get('user', 'root'),
                               password=kwargs.get('password', '123456'),
                               db=kwargs.get('db', 'test'),
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
    print(sql_insert_data)
    execute_sql(conn, sql_insert_data)
    conn.commit()


def main():
    conn = get_conn(host="localhost",
                    user="root",
                    password="123456")

    test_data = [[9999120, "tset01", 1234, 2022, "蓝鲸123", 5678, 9999, "2022-05-07"],
                 [9999121, "tset02", 1235, 2023, "蓝鲸124", 5679, 9998, "2022-05-07"],
                 [9999122, "tset03", 1236, 2024, "蓝鲸125", 5680, 9997, "2022-05-07"]]
    try:
        for i in test_data:
            insert_data(conn=conn,
                        bk_biz_id=i[0],
                        bk_biz_name=i[1],
                        administrator_id=i[2],
                        bk_sops_id=i[3],
                        bk_sops_name=i[4],
                        DevOps_group_id=i[5],
                        view_group_id=i[6],
                        create_time=i[7])
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    main()
