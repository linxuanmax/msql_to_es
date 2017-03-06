#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="yanzl"
__date__="2017-02-27"

'''
连接数据库的信息:为字典形式
例：  mysql1:数据库连接的配置（键）名称
     hostname:IP地址     port:端口     dbname:连接的数据库的名称    username:数据库的用户名     userpass:与用户名对应的密码
'''
dic_connection = {
    'mysql1': {'hostname': '192.168.1.225', 'port': '3306', 'dbname': 'test_a', 'username': 'root', 'userpass': 'MTIzNDY1'},
    'mysql2': {'hostname': '192.168.1.225', 'port': '3306', 'dbname': 'test_b', 'username': 'root', 'userpass': 'MTIzNDY1'},
    'mysql3': {'hostname': '192.168.1.225', 'port': '3306', 'dbname': 'test_c', 'username': 'root', 'userpass': 'MTIzNDY1'}
}

'''
任务列表:列表形式
taskname:任务名称(命名方式：mysql_first)        dbic:任务标号          reclimit:限制每次导入的记录数目
sql:提取数据sql语句        timefld:时间增量字段   incfld:主键字段    tbname:数据表的名称
conname:数据库连接配置名称  tbstructure:数据表的字段（主键放在第一位，时间增量字段放在最后一位）
inctype:数据的增长方式（1：主键增量 2：时间增量 0：一次性导入）   mianincr:主键字段类型
'''

dic_tasklist = [
    {'taskname': '1_1', 'dbid': 'mysql1', 'reclimit': '5', 'inctype': 1, 'timefld':'', 'incrfld':'ID', 'mianincr': 'number','msgfld':123, 'curpos':'', 'tbname':'info_aa',
     'conname': 'mysql1', 'sql': 'select %s from %s where %s > %s order by %s limit %s ;', 'tbstructure':['ID', 'name']},

    {'taskname': '1_2', 'dbid': 'mysql1', 'reclimit': '5', 'inctype': 1, 'timefld': '', 'incrfld': 'ID', 'mianincr': 'string', 'msgfld': 123, 'curpos': '', 'tbname': 'info_ba',
     'conname': 'mysql1', 'sql': 'select %s from %s where %s > %s order by %s limit %s ;','tbstructure': ['ID', 'name']},

    {'taskname': '1_3', 'dbid': 'mysql1', 'reclimit': '2', 'inctype': 1, 'timefld': '', 'incrfld': 'ID', 'mianincr': 'time', 'msgfld': 123, 'curpos': '', 'tbname': 'info_ca',
     'conname': 'mysql1', 'sql': 'select %s from %s where %s > %s order by %s limit %s ;', 'tbstructure': ['ID', 'name']},

    {'taskname': '2_1', 'dbid': 2, 'reclimit': '10', 'inctype': 2, 'timefld': 'curtime', 'incrfld': 'ID', 'mianincr': 'number','msgfld': 'curtime', 'tbname':'info_ab',
     'conname': 'mysql2', 'sql': 'select %s from %s where %s >  %s  and %s > "'" %s "'" and %s <= "'" %s "'" order by %s limit %s ;', 'tbstructure': ['ID', 'curtime']},

    {'taskname': '2_2', 'dbid': 3, 'reclimit': '10', 'inctype': 2, 'timefld': 'curtime', 'incrfld': 'ID', 'mianincr': 'string','msgfld': 'curtime',  'tbname':'info_bb',
     'conname': 'mysql2', 'sql': 'select * from %s where %s >  "%s" and %s <= "'" %s "'" and %s > "'" %s "'" order by %s limit %s ;', 'tbstructure': ['ID', 'curtime']},

    {'taskname': '2_3', 'dbid': 4, 'reclimit': '10', 'inctype': 2, 'timefld': 'curtime', 'incrfld': 'ID','mianincr': 'time','msgfld': 'curtime','tbname':'info_cb',
     'conname': 'mysql2', 'sql': 'select * from %s where %s >  %s and %s <= "'" %s "'" and %s > "'" %s "'" order by %s limit %s', 'tbstructure': ['ID', 'curtime']},

    {'taskname': '0_1', 'dbid': 5, 'reclimit': '10', 'inctype': 0, 'timefld': '', 'incrfld': 'ID', 'mianincr': 'number', 'msgfld': '' ,'curpos':'', 'tbname':'info_ac',
     'conname': 'mysql3', 'sql': 'select %s from %s ;', 'tbstructure': ['ID', 'name']},
]



