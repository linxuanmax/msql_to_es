# -*- coding=utf-8 -*-

import pyes
from pyes import *
from pytz import timezone
from datetime import *
import time
import os
import traceback
from mdstack.utils import sys_config, sys_log
import es_stats
import ipaddress
import post_data


def create_index(conn, idx_name, doc_type):
    if not conn.indices.exists_index(idx_name):
        conn.indices.create_index(idx_name)
        mapping = {
            "properties": {
                "time":{"type": "date"}
            }
        }
        conn.indices.put_mapping(doc_type, mapping, [idx_name])


def flows_stats_per_hour(idx_name, doc_type, conn, day, localtz, base_condition):
    """
    功能：统计昨天、前天每个小时内网络流量的session次数、包数、字节数、数据字节数、时延
    字段：timestamp, time, week_hour, hour, by, by1, by2, db, db1, db2, pa, pa1, pa2, count
    使用：flows-dashboard-01/flows-dashboard-02
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "sessions-" + day.strftime("%y%m%d")
    if not conn.indices.exists_index(idx_name_o):
        #raise Exception("索引 " + idx_name_o + " 不存在！")
        return

    p_string_query = {}
    p_string_query["query"] = "*"
    if base_condition != "":
        if p_string_query["query"] == "*":
            p_string_query["query"] = base_condition
        else:
            p_string_query["query"] += " AND " + base_condition
    p_string_query["default_field"] = "by"
    p_string_query["default_operator"] = "AND"
    p_range_filter = {}
    p_range_filter["range_field"] = "lp"
    p_range_filter["include_upper"] = True
    p_range_filter["include_lower"] = True
    p_statistical = {}
    p_statistical["name"] = "results"

    # 分24个小时，循环统计
    for i in xrange(0, 24):
        p_range_filter["from_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))).timetuple())
        p_range_filter["to_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))).timetuple())

        # todo: 缺时延
        fields = {"by":"bytes", "by1":"bytes_src", "by2":"bytes_dst", "db":"databytes", "db1":"databytes_src", "db2":"databytes_dst", "pa":"packets", "pa1":"packets_src", "pa2":"packets_dst"}
        doc = {}
        for field in fields.keys():
            p_statistical["field"] = field
            rs = es_stats.statistical(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, statistical=p_statistical)

            if rs.facets.has_key(p_statistical["name"]):
                # 合并结果集
                #doc[fields[field]] = rs.facets[p_statistical["name"]]["total"]
                doc[field] = rs.facets[p_statistical["name"]]["total"]
                doc["count"] = rs.facets[p_statistical["name"]]["count"]

        # 保存
        t = datetime(day.year, day.month, day.day, i, 0, 0)
        doc["hour"] = t.strftime("%H")
        doc["week_hour"] = t.strftime("%w_%H")
        doc["time"] = localtz.localize(t)
        doc["timestamp"] = p_range_filter["from_value"]
        # 指定id，这样可以自动更新已经统计过的数据
        docid = t.strftime("%y%m%d%H")

        try:
            conn.index(doc, idx_name, doc_type, docid)
        except Exception, e:
            pass


def flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition):
    """
    功能：统计昨天、前天的某个字段的分组统计（count）,并统计每个分组值当天的分时数量（count）
    字段：top (timestamp, time, week, key_field, value_field, is_top, field, total)
          detail (timestamp, time, week_hour, hour, key_field, value_field, is_top, field, total)
    使用：fw-dashboard-03
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "sessions-" + day.strftime("%y%m%d")
    if not conn.indices.exists_index(idx_name_o):
        #raise Exception("索引 " + idx_name_o + " 不存在！")
        return

    p_string_query = {}
    p_string_query["query"] = "*"
    if base_condition != "":
        if p_string_query["query"] == "*":
            p_string_query["query"] = base_condition
        else:
            p_string_query["query"] += " AND " + base_condition
    p_string_query["default_field"] = "by"
    p_string_query["default_operator"] = "AND"
    p_range_filter = {}
    p_range_filter["range_field"] = "lp"
    p_range_filter["include_upper"] = True
    p_range_filter["include_lower"] = True
    p_term_facet = {}
    p_term_facet["field"] = field
    p_term_facet["name"] = "results"
    p_term_facet["size"] = size
    p_term_facet["order"] = "count"

    #统计全天的top
    p_range_filter["from_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))).timetuple())
    p_range_filter["to_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))).timetuple())
    rs = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, term_facet=p_term_facet)

    if rs.facets.has_key(p_term_facet["name"]):
        # 保存top结果
        for doc in rs.facets[p_term_facet["name"]]["terms"]:
            t = datetime(day.year, day.month, day.day, 0, 0, 0)
            doc["timestamp"] = p_range_filter["from_value"]
            doc["week"] = t.strftime("%w")
            doc["time"] = localtz.localize(t)
            if field == "a1" or field == "a2":
                    doc["field"] = str(ipaddress.ip_address(doc["term"]))
            else:
                doc["field"] = doc["term"]
            # 后面详细统计时要用到
            tmp_term = doc["term"]
            # "Y"表示存储的是TOP值
            doc["is_top"] = "Y"
            doc["key_field"] = field
            doc["value_field"] = "session"
            doc["total"] = doc["count"]
            del doc["term"]
            del doc["count"]
            docid = doc["key_field"] + "_" + doc["value_field"] + "_" + str(doc["field"]) + "_" + t.strftime("%y%m%d")

            # 指定id，这样可以自动更新已经统计过的数据
            try:
                conn.index(doc, idx_name, doc_type, docid)
            except Exception, e:
                pass

            # 统计分组值的分时数量，循环统计
            p_string_query_detail = {}
            p_string_query_detail["query"] = field + ":\"" + str(tmp_term) + "\""
            if base_condition != "":
                p_string_query_detail["query"] += " AND " + base_condition
            p_string_query_detail["default_field"] = "by"
            p_string_query_detail["default_operator"] = "AND"
            p_range_filter_detail = {}
            p_range_filter_detail["range_field"] = "lp"
            p_range_filter_detail["include_upper"] = True
            p_range_filter_detail["include_lower"] = True
            p_term_facet_detail = {}
            p_term_facet_detail["name"] = "results"
            p_term_facet_detail["field"] = field
            p_term_facet_detail["size"] = size
            p_term_facet_detail["order"] = "count"
            for i in xrange(0, 24):
                p_range_filter_detail["from_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))).timetuple())
                p_range_filter_detail["to_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))).timetuple())
                print '================='
                rs_detail = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query_detail, range_filter=p_range_filter_detail, term_facet=p_term_facet_detail)
                if rs_detail.facets.has_key(p_term_facet["name"]):
                    t_detail = datetime(day.year, day.month, day.day, i, 0, 0)
                    doc_detail = {}
                    if len(rs_detail.facets[p_term_facet["name"]]["terms"]) > 0:
                        for doc_detail in rs_detail.facets[p_term_facet["name"]]["terms"]:
                            doc_detail["hour"] = t_detail.strftime("%H")
                            doc_detail["week_hour"] = t_detail.strftime("%w_%H")
                            doc_detail["timestamp"] = p_range_filter_detail["from_value"]
                            doc_detail["time"] = localtz.localize(t_detail)
                            ##doc_detail["field"] = doc_detail["term"]
                            
                            doc_detail["key_field"] = doc["key_field"]
                            doc_detail["value_field"] = "session"
                            doc_detail["field"] = doc["field"]
                            doc_detail["total"] = doc_detail["count"]
                            # "Y"表示存储的是TOP值，"N"表示存储的是分时24小时的值
                            doc_detail["is_top"] = "N"
                            del doc_detail["term"]
                            del doc_detail["count"]
                            #docid_detail = doc_detail["key_field"] + "_" + doc_detail["value_field"] + "_" + str(doc_detail["field"]) + "_" + t_detail.strftime("%y%m%d%H")
                            #print docid_detail, doc_detail
                            # 指定id，这样可以自动更新已经统计过的数据
                            """
                            try:
                                conn.index(doc_detail, idx_name, doc_type, docid_detail)
                            except Exception, e:
                                pass
                            """
                    else:
                        doc_detail["hour"] = t_detail.strftime("%H")
                        doc_detail["week_hour"] = t_detail.strftime("%w_%H")
                        doc_detail["timestamp"] = p_range_filter_detail["from_value"]
                        doc_detail["time"] = localtz.localize(t_detail)
                        doc_detail["key_field"] = doc["key_field"]
                        doc_detail["value_field"] = "session"
                        doc_detail["field"] = doc["field"]
                        doc_detail["total"] = 0
                        doc_detail["is_top"] = "N"
                    docid_detail = doc_detail["key_field"] + "_" + doc_detail["value_field"] + "_" + str(doc_detail["field"]) + "_" + t_detail.strftime("%y%m%d%H")
                    print docid_detail, doc_detail

                    # 指定id，这样可以自动更新已经统计过的数据
                    try:
                        conn.index(doc_detail, idx_name, doc_type, docid_detail)
                    except Exception, e:
                        pass


def flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition):
    """
    功能：统计昨天、前天的某个字段的分组统计（sum）,并统计每个分组值当天的分时数量（sum）
    字段：top (timestamp, time, week, key_field, value_field, is_top, field, total)
          detail (timestamp, time, week_hour, hour, key_field, value_field, is_top, field, total)
    使用：fw-dashboard-03
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "sessions-" + day.strftime("%y%m%d")
    if not conn.indices.exists_index(idx_name_o):
        #raise Exception("索引 " + idx_name_o + " 不存在！")
        return

    p_string_query = {}
    p_string_query["query"]  = "*"
    if base_condition != "":
        if p_string_query["query"] == "*":
            p_string_query["query"] = base_condition
        else:
            p_string_query["query"] += " AND " + base_condition
    p_string_query["default_field"] = "by"
    p_string_query["default_operator"] = "AND"
    p_range_filter = {}
    p_range_filter["range_field"] = "lp"
    p_range_filter["include_upper"] = True
    p_range_filter["include_lower"] = True
    p_term_stats = {}
    p_term_stats["name"] = "results"
    p_term_stats["key_field"] = key_field
    p_term_stats["size"] = size
    p_term_stats["order"] = "total"

    for value_field in value_fields:
        p_term_stats["value_field"] = value_field
        #统计全天的top
        p_range_filter["from_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))).timetuple())
        p_range_filter["to_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))).timetuple())
        rs = es_stats.term_stats(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, term_stats=p_term_stats)

        if rs.facets.has_key(p_term_stats["name"]):
            # 保存top结果
            for doc in rs.facets[p_term_stats["name"]]["terms"]:
                t = datetime(day.year, day.month, day.day, 0, 0, 0)
                doc["timestamp"] = p_range_filter["from_value"]
                doc["time"] = localtz.localize(t)
                doc["week"] = t.strftime("%w")
                doc["key_field"] = p_term_stats["key_field"]
                doc["value_field"] = p_term_stats["value_field"]
                if key_field == "a1" or key_field == "a2":
                    doc["field"] = str(ipaddress.ip_address(doc["term"]))
                else:
                    doc["field"] = doc["term"]
                # 后面详细统计时要用到
                tmp_term = doc["term"]
                # "Y"表示存储的是TOP值
                doc["is_top"] = "Y"
                del doc["term"]
                del doc["max"]
                del doc["min"]
                del doc["mean"]
                del doc["count"]
                del doc["total_count"]
                docid = doc["key_field"] + "_" + doc["value_field"] + "_" + str(doc["field"]) + "_" + t.strftime("%y%m%d")
                # 指定id，这样可以自动更新已经统计过的数据
                conn.index(doc, idx_name, doc_type, docid)

                # 统计分组值的分时数量，循环统计
                p_string_query_detail = {}
                p_string_query_detail["query"] = key_field + ":\"" + str(tmp_term) + "\""
                if base_condition != "":
                    p_string_query_detail["query"] += " AND " + base_condition
                p_string_query_detail["default_field"] = "by"
                p_string_query_detail["default_operator"] = "AND"
                p_range_filter_detail = {}
                p_range_filter_detail["range_field"] = "lp"
                p_range_filter_detail["include_upper"] = True
                p_range_filter_detail["include_lower"] = True
                p_statistical_detail = {}
                p_statistical_detail["name"] = "results"
                p_statistical_detail["field"] = value_field

                for i in xrange(0, 24):
                    p_range_filter_detail["from_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))).timetuple())
                    p_range_filter_detail["to_value"] = time.mktime((localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))).timetuple())
                    rs_detail = es_stats.statistical(conn, idx_name_o, string_query=p_string_query_detail, range_filter=p_range_filter_detail, statistical=p_statistical_detail)

                    if rs_detail.facets.has_key(p_statistical_detail["name"]):
                        doc_detail = {}
                        t_detail = datetime(day.year, day.month, day.day, i, 0, 0)
                        doc_detail["hour"] = t_detail.strftime("%H")
                        doc_detail["week_hour"] = t_detail.strftime("%w_%H")
                        doc_detail["timestamp"] = p_range_filter_detail["from_value"]
                        doc_detail["time"] = localtz.localize(t_detail)
                        doc_detail["key_field"] = doc["key_field"]
                        doc_detail["value_field"] = doc["value_field"]
                        doc_detail["field"] = doc["field"]
                        doc_detail["total"] = rs_detail.facets[p_statistical_detail["name"]]["total"]
                        # "Y"表示存储的是TOP值，"N"表示存储的是分时24小时的值
                        doc_detail["is_top"] = "N"
                        docid_detail = doc_detail["key_field"] + "_" + doc_detail["value_field"] + "_" + str(doc_detail["field"]) + "_" + t_detail.strftime("%y%m%d%H")
                        # 指定id，这样可以自动更新已经统计过的数据
                        print doc_detail

                        try:
                            conn.index(doc_detail, idx_name, doc_type, docid_detail)
                        except Exception, e:
                            pass


def fw_geo_count_per_day(idx_name, doc_type, conn, day, localtz, base_condition):
    """
    功能：统计昨天、前天的国家、城市的分组统计（count）
    字段：
    使用：fw-dashboard-07
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "sessions-" + day.strftime("%Y.%m.%d")
    if not conn.indices.exists_index(idx_name_o): return
    
    # 通过、拒绝
    actions = ["pass", "deny"]
    for action in actions:
        p_string_query_country = {}
        p_string_query_country["query"] = "gw_action:\"" + action + "\""
        if base_condition != "":
            p_string_query_country["query"] += " AND " + base_condition
        p_string_query_country["default_field"] = "msg.message"
        p_string_query_country["default_operator"] = "AND"
        p_range_filter_country = {}
        p_range_filter_country["range_field"] = "@timestamp"
        p_range_filter_country["include_upper"] = True
        p_range_filter_country["include_lower"] = True
        p_term_facet_country = {}
        p_term_facet_country["name"] = "results"
        p_term_facet_country["field"] = "gw_dst_country"
        p_term_facet_country["size"] = 500
        p_term_facet_country["order"] = "count"

        #统计全天的国家
        p_range_filter_country["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        p_range_filter_country["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))
        rs_country = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query_country, range_filter=p_range_filter_country, term_facet=p_term_facet_country)
        
        if rs_country.facets.has_key(p_term_facet_country["name"]):
            for country in rs_country.facets[p_term_facet_country["name"]]["terms"]:
                p_string_query_city = {}
                p_string_query_city["query"] = "gw_action:\"" + action + "\" AND gw_dst_country:\"" + country["term"] + "\""
                if base_condition != "":
                    p_string_query["query"] += " AND " + base_condition
                p_string_query_city["default_field"] = "msg.message"
                p_string_query_city["default_operator"] = "AND"
                p_range_filter_city = {}
                p_range_filter_city["range_field"] = "@timestamp"
                p_range_filter_city["include_upper"] = True
                p_range_filter_city["include_lower"] = True
                p_term_facet_city = {}
                p_term_facet_city["name"] = "results"
                p_term_facet_city["field"] = "gw_dst_city"
                p_term_facet_city["size"] = 500
                p_term_facet_city["order"] = "count"
                
                #统计某个国家全天的所有城市
                p_range_filter_city["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
                p_range_filter_city["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))
                rs_city = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query_city, range_filter=p_range_filter_city, term_facet=p_term_facet_city)
                
                if rs_city.facets.has_key(p_term_facet_city["name"]):
                    for doc in rs_city.facets[p_term_facet_city["name"]]["terms"]:
                        t = datetime(day.year, day.month, day.day, 0, 0, 0)
                        doc["week"] = t.strftime("%w")
                        doc["time"] = localtz.localize(t)
                        doc["country"] = country["term"]
                        doc["city"] = doc["term"]
                        doc["action"] = action
                        del doc["term"]
                        docid = country["term"] + "_" + doc["city"] + "_" + t.strftime("%y%m%d") + "_" + action
                        # 指定id，这样可以自动更新已经统计过的数据
                        try:
                            conn.index(doc, idx_name, doc_type, docid)
                        except Exception, e:
                            pass


def clear_index(idx_name, doc_type, conn, days):
    """
    删除过期的统计数据
    """

    if not conn.indices.exists_index(idx_name):
        return
    day = datetime.now() - timedelta(days)
    localtz = timezone('Asia/Shanghai')
    s_date = localtz.localize(datetime(2000, 1, 1, 0, 0, 0))
    e_date = localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))
    # RangeQuery查询
    query = pyes.query.RangeQuery(pyes.utils.ESRange("time", from_value=s_date, to_value=e_date, include_upper=True, include_lower=True))
    path = conn._make_path(idx_name, doc_type, '_query')
    body = '{"query":' + conn._encode_query(query) + '}'
    conn._send_request('DELETE', path, body, None)


def stats_flows_dashboard():
    """
    自定义流量dashboard，统一调用入口
    """
    
    # 配置文件
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
    conf = sys_config.SysConfig(configFile)
    logFile = conf.getConfig("datastats", "logFile")
    instance = conf.getConfig("datastats", "instanceName")
    es_host = conf.getConfig("elasticsearch", "esHost")
    es_port = conf.getConfig("elasticsearch", "esPort")

    post_host = conf.getConfig("postgresql", "dbHost")
    post_port = conf.getConfig("postgresql", "dbPort")
    post_user = conf.getConfig("postgresql", "dbUser")
    post_pwd = conf.getConfig("postgresql", "dbPwd")
    post_db = conf.getConfig("postgresql", "dbName")

    # 删除过期的统计数据
    post_url = "host=" + post_host + " port=" + post_port  + " user=" + post_user + " password=" + post_pwd + " dbname=" + post_db
    days = post_data.get_expiry_date(post_url, "flows") + 1

    idx_name = "flows_dashboard"
    url = 'http://' + es_host + ":" + es_port
    print url
    conn = ES(url, timeout = 120)
    localtz = timezone('Asia/Shanghai')
    # 基础查询条件，这里一般设置通用查询的条件
    base_condition = ""

    # 统计昨天、前天每个小时内网络流量的session次数、包数、字节数、数据字节数、时延
    if conf.getConfig("custom_dashboard", "flows.flows_stats_per_hour") == "Y":
        try:
            doc_type = "flows_stats_per_hour"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            day = datetime.now() - timedelta(1)
            flows_stats_per_hour(idx_name, doc_type, conn, day, localtz, base_condition)
            day = datetime.now() - timedelta(2)
            flows_stats_per_hour(idx_name, doc_type, conn, day, localtz,base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"源IP"的"Session数量"的TOPN，并分别统计"源IP"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_srcip_count_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "a1"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"源IP"的"字节数/源IP发送字节数/目的IP发送字节数/数据字节数/源IP发送数据字节数/目的IP发送数据字节数/数据包数/源IP发送数据包数/目的IP发送数据包数"的TOPN
    # 并分别统计"源IP"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_srcip_sum_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            key_field = "a1"
            value_fields = ["by", "by1", "by2", "db", "db1", "db2", "pa", "pa1", "pa2"]
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"目的IP"的"Session数量"的TOPN，并分别统计"目的IP"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_dstip_count_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "a2"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"目的IP"的"字节数/源IP发送字节数/目的IP发送字节数/数据字节数/源IP发送数据字节数/目的IP发送数据字节数/数据包数/源IP发送数据包数/目的IP发送数据包数"的TOPN
    # 并分别统计"目的IP"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_dstip_sum_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            key_field = "a2"
            value_fields = ["by", "by1", "by2", "db", "db1", "db2", "pa", "pa1", "pa2"]
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"目的端口"的"Session数量"的TOPN，并分别统计"目的端口"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_dstport_count_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "p2"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计每天网络流量中"目的端口"的"字节数/源IP发送字节数/目的IP发送字节数/数据字节数/源IP发送数据字节数/目的IP发送数据字节数/数据包数/源IP发送数据包数/目的IP发送数据包数"的TOPN
    # 并分别统计"目的端口"TOPN中的各统计项的24小时分时值
    if conf.getConfig("custom_dashboard", "flows.flows_dstport_sum_per_day") == "Y":
        try:
            doc_type = "flows_top_detail_stats_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            key_field = "p2"
            value_fields = ["by", "by1", "by2", "db", "db1", "db2", "pa", "pa1", "pa2"]
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
            day = datetime.now() - timedelta(2)
            flows_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_fields, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    """
    # 统计国家、城市在每天内被pass/deny的次数的TOPN
    if conf.getConfig("custom_dashboard", "firewall.fw_geo_count_per_day") == "Y":
        try:
            doc_type = "fw_geo_count_per_day"
            day = datetime.now() - timedelta(1)
            fw_geo_count_per_day(idx_name, doc_type, conn, day, localtz, base_condition)
            day = datetime.now() - timedelta(2)
            fw_geo_count_per_day(idx_name, doc_type, conn, day, localtz, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))
    """

if __name__ == "__main__":
    """
    测试
    """
    
    stats_flows_dashboard()
