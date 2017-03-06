# -*- coding=utf-8 -*-

import pyes
from pyes import *
from pytz import timezone
from datetime import *
import os
import traceback
from mdstack.utils import sys_config, sys_log
import es_stats
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


def fw_stats_per_hour(idx_name, doc_type, conn, day, localtz, base_condition):
    """
    功能：统计昨天、前天每个小时内各台防火墙的pass次数、deny次数、发送字节数、接收字节数、总字节数、发送包数、接收包数、总包数
    字段：time, ip, flag, action, week_hour, hour, sent_packets, recv_packets, packets, sent_bytes, recv_bytes, bytes, count
    使用：fw-dashboard-01/fw-dashboard-02/fw-dashboard-03/fw-dashboard-06
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "datagroup-" + day.strftime("%Y.%m.%d")
    if not conn.indices.exists_index(idx_name_o): return

    # 通过、拒绝
    actions = ["pass", "deny"]
    for action in actions:
        p_string_query = {}
        p_string_query["query"] = "gw_action:\"" + action + "\""
        if base_condition != "":
            p_string_query["query"] += " AND " + base_condition
        p_string_query["default_field"] = "msg.message"
        p_string_query["default_operator"] = "AND"
        p_range_filter = {}
        p_range_filter["range_field"] = "@timestamp"
        p_range_filter["include_upper"] = True
        p_range_filter["include_lower"] = True
        p_term_stats = {}
        p_term_stats["name"] = "results"
        p_term_stats["key_field"] = "msg.host"
        p_term_stats["size"] = 500
        p_term_stats["order"] = "term"

        # 分24个小时，循环统计
        for i in xrange(0, 24):
            p_range_filter["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))
            p_range_filter["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))
            value_fields = ["gw_recv_bytes", "gw_sent_bytes", "gw_bytes", "gw_recv_packets", "gw_sent_packets", "gw_packets"]
            docs = {}
            for field in value_fields:
                p_term_stats["value_field"] = field
                rs = es_stats.term_stats(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, term_stats=p_term_stats)

                if rs.facets.has_key(p_term_stats["name"]):
                    # 合并结果集
                    for doc in rs.facets[p_term_stats["name"]]["terms"]:
                        if not docs.has_key(doc["term"]):
                            docs[doc["term"]] = {}
                        docs[doc["term"]][field[3:]] = doc["total"]
                        docs[doc["term"]]["count"] = doc["count"]
            # 保存
            t = datetime(day.year, day.month, day.day, i, 0, 0)
            for ip in docs:
                docs[ip]["hour"] = t.strftime("%H")
                docs[ip]["week_hour"] = t.strftime("%w_%H")
                docs[ip]["time"] = localtz.localize(t)
                docs[ip]["ip"] = ip
                docs[ip]["action"] = action
                # 指定id，这样可以自动更新已经统计过的数据
                docid = ip + "_" + t.strftime("%y%m%d%H") + "_" + action
                try:
                    conn.index(docs[ip], idx_name, doc_type, docid)
                except Exception, e:
                    pass


def fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition):
    """
    功能：统计昨天、前天的某个字段的分组统计（count）,并统计每个分组值当天的分时数量（count）
    字段：top (time, week, action, is_top, field, count)
          detail (time, week_hour, hour, action, is_top, field, count)
    使用：fw-dashboard-04
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "datagroup-" + day.strftime("%Y.%m.%d")
    if not conn.indices.exists_index(idx_name_o): return
    
    # 通过、拒绝
    actions = ["pass", "deny"]
    for action in actions:
        p_string_query = {}
        p_string_query["query"] = "gw_action:\"" + action + "\""
        if base_condition != "":
            p_string_query["query"] += " AND " + base_condition
        p_string_query["default_field"] = "msg.message"
        p_string_query["default_operator"] = "AND"
        p_range_filter = {}
        p_range_filter["range_field"] = "@timestamp"
        p_range_filter["include_upper"] = True
        p_range_filter["include_lower"] = True
        p_term_facet = {}
        p_term_facet["name"] = "results"
        p_term_facet["field"] = field
        p_term_facet["size"] = size
        p_term_facet["order"] = "count"
        
        #统计全天的top
        p_range_filter["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        p_range_filter["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))
        rs = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, term_facet=p_term_facet)

        if rs.facets.has_key(p_term_facet["name"]):
            # 保存top结果
            for doc in rs.facets[p_term_facet["name"]]["terms"]:
                t = datetime(day.year, day.month, day.day, 0, 0, 0)
                doc["week"] = t.strftime("%w")
                doc["time"] = localtz.localize(t)
                doc["field"] = doc["term"]
                doc["action"] = action
                # "Y"表示存储的是TOP值
                doc["is_top"] = "Y"
                del doc["term"]
                docid = doc["field"] + "_" + t.strftime("%y%m%d") + "_" + action
                # 指定id，这样可以自动更新已经统计过的数据
                try:
                    conn.index(doc, idx_name, doc_type, docid)
                except Exception, e:
                    pass
                
                # 统计分组值的分时数量，循环统计
                p_string_query_detail = {}
                p_string_query_detail["query"] = field + ":\"" + doc["field"] + "\" AND " + "gw_action:\"" + action + "\""
                if base_condition != "":
                    p_string_query_detail["query"] += " AND " + base_condition
                p_string_query_detail["default_field"] = "msg.message"
                p_string_query_detail["default_operator"] = "AND"
                p_range_filter_detail = {}
                p_range_filter_detail["range_field"] = "@timestamp"
                p_range_filter_detail["include_upper"] = True
                p_range_filter_detail["include_lower"] = True
                p_term_facet_detail = {}
                p_term_facet_detail["name"] = "results"
                p_term_facet_detail["field"] = field
                p_term_facet_detail["size"] = size
                p_term_facet_detail["order"] = "count"
                for i in xrange(0, 24):
                    p_range_filter_detail["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))
                    p_range_filter_detail["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))
                    rs_detail = es_stats.term_facet(conn, idx_name_o, string_query=p_string_query_detail, range_filter=p_range_filter_detail, term_facet=p_term_facet_detail)
                    
                    if rs_detail.facets.has_key(p_term_facet["name"]):
                        for doc_detail in rs_detail.facets[p_term_facet["name"]]["terms"]:
                            t_detail = datetime(day.year, day.month, day.day, i, 0, 0)
                            doc_detail["hour"] = t_detail.strftime("%H")
                            doc_detail["week_hour"] = t_detail.strftime("%w_%H")
                            doc_detail["time"] = localtz.localize(t_detail)
                            doc_detail["field"] = doc_detail["term"]
                            doc_detail["action"] = action
                            # "Y"表示存储的是TOP值，"N"表示存储的是分时24小时的值
                            doc_detail["is_top"] = "N"
                            del doc_detail["term"]
                            docid_detail = doc_detail["field"] + "_" + t_detail.strftime("%y%m%d%H") + "_" + action
                            # 指定id，这样可以自动更新已经统计过的数据
                            try:
                                conn.index(doc_detail, idx_name, doc_type, docid_detail)
                            except Exception, e:
                                pass


def fw_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_field, size, base_condition):
    """
    功能：统计昨天、前天的某个字段的分组统计（sum）,并统计每个分组值当天的分时数量（sum）
    字段：top (time, week, action, is_top, field, total)
          detail (time, week_hour, hour, action, is_top, field, total)
    使用：fw-dashboard-05
    """

    create_index(conn, idx_name, doc_type)
    idx_name_o = "datagroup-" + day.strftime("%Y.%m.%d")
    if not conn.indices.exists_index(idx_name_o): return
    
    # 通过、拒绝
    actions = ["pass", "deny"]
    for action in actions:
        p_string_query = {}
        p_string_query["query"] = "gw_action:\"" + action + "\""
        if base_condition != "":
            p_string_query["query"] += " AND " + base_condition
        p_string_query["default_field"] = "msg.message"
        p_string_query["default_operator"] = "AND"
        p_range_filter = {}
        p_range_filter["range_field"] = "@timestamp"
        p_range_filter["include_upper"] = True
        p_range_filter["include_lower"] = True
        p_term_stats = {}
        p_term_stats["name"] = "results"
        p_term_stats["key_field"] = key_field
        p_term_stats["value_field"] = value_field
        p_term_stats["size"] = size
        p_term_stats["order"] = "total"
        
        #统计全天的top
        p_range_filter["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, 0, 0, 0))
        p_range_filter["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, 23, 59, 59))
        rs = es_stats.term_stats(conn, idx_name_o, string_query=p_string_query, range_filter=p_range_filter, term_stats=p_term_stats)

        if rs.facets.has_key(p_term_stats["name"]):
            # 保存top结果
            for doc in rs.facets[p_term_stats["name"]]["terms"]:
                t = datetime(day.year, day.month, day.day, 0, 0, 0)
                doc["week"] = t.strftime("%w")
                doc["time"] = localtz.localize(t)
                doc["field"] = doc["term"]
                doc["action"] = action
                # "Y"表示存储的是TOP值
                doc["is_top"] = "Y"
                del doc["term"]
                del doc["max"]
                del doc["min"]
                del doc["mean"]
                del doc["count"]
                del doc["total_count"]
                docid = doc["field"] + "_" + t.strftime("%y%m%d") + "_" + action
                # 指定id，这样可以自动更新已经统计过的数据
                conn.index(doc, idx_name, doc_type, docid)
                
                # 统计分组值的分时数量，循环统计
                p_string_query_detail = {}
                p_string_query_detail["query"] = key_field + ":\"" + doc["field"] + "\" AND " + "gw_action:\"" + action + "\""
                if base_condition != "":
                    p_string_query_detail["query"] += " AND " + base_condition
                p_string_query_detail["default_field"] = "msg.message"
                p_string_query_detail["default_operator"] = "AND"
                p_range_filter_detail = {}
                p_range_filter_detail["range_field"] = "@timestamp"
                p_range_filter_detail["include_upper"] = True
                p_range_filter_detail["include_lower"] = True
                p_statistical_detail = {}
                p_statistical_detail["name"] = "results"
                p_statistical_detail["field"] = value_field
                
                for i in xrange(0, 24):
                    p_range_filter_detail["from_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 0, 0))
                    p_range_filter_detail["to_value"] = localtz.localize(datetime(day.year, day.month, day.day, i, 59, 59))
                    rs_detail = es_stats.statistical(conn, idx_name_o, string_query=p_string_query_detail, range_filter=p_range_filter_detail, statistical=p_statistical_detail)
                    
                    if rs_detail.facets.has_key(p_statistical_detail["name"]):
                        doc_detail = {}
                        t_detail = datetime(day.year, day.month, day.day, i, 0, 0)
                        doc_detail["hour"] = t_detail.strftime("%H")
                        doc_detail["week_hour"] = t_detail.strftime("%w_%H")
                        doc_detail["time"] = localtz.localize(t_detail)
                        doc_detail["field"] = doc["field"]
                        doc_detail["action"] = action
                        doc_detail["total"] = rs_detail.facets[p_statistical_detail["name"]]["total"]
                        # "Y"表示存储的是TOP值，"N"表示存储的是分时24小时的值
                        doc_detail["is_top"] = "N"
                        docid_detail = doc_detail["field"] + "_" + t_detail.strftime("%y%m%d%H") + "_" + action
                        # 指定id，这样可以自动更新已经统计过的数据
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
    idx_name_o = "datagroup-" + day.strftime("%Y.%m.%d")
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
                    p_string_query_city["query"] += " AND " + base_condition
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


def stats_firewall_dashboard():
    """
    自定义防火墙dashboard，统一调用入口
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

    idx_name = "firewall_dashboard"
    url = 'http://' + es_host + ":" + es_port
    print url
    conn = ES(url, timeout = 120)
    localtz = timezone('Asia/Shanghai')
    # 基础查询条件，这里一般设置查询防火墙的条件
    base_condition = "(msg.dn:\"hw:venustech:usg_fw_3610d\")"

    # 统计各台防火墙在每个小时内被pass/deny的访问次数、字节（发送/接收/总数）、数据包（发送/接收/总数）
    if conf.getConfig("custom_dashboard", "firewall.fw_stats_per_hour") == "Y":
        try:
            doc_type = "fw_stats_per_hour"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            day = datetime.now() - timedelta(1)
            fw_stats_per_hour(idx_name, doc_type, conn, day, localtz, base_condition)
            day = datetime.now() - timedelta(2)
            fw_stats_per_hour(idx_name, doc_type, conn, day, localtz,base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计"协议+端口"在每天内被pass/deny的次数的TOPN，并统计TOPN中的各"协议+端口"的24小时分时次数
    if conf.getConfig("custom_dashboard", "firewall.fw_proto_port_count_per_day") == "Y":
        try:
            doc_type = "fw_proto_port_count_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "gw_proto_port"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计"源IP"在每天内被pass/deny的次数的TOPN，并统计TOPN中的各"源IP"的24小时分时次数
    if conf.getConfig("custom_dashboard", "firewall.fw_srcip_count_per_day") == "Y":
        try:
            doc_type = "fw_srcip_count_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "gw_src_ipv4"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计"IP对"在每天内被pass/deny的次数的TOPN，并统计TOPN中的各"IP对"的24小时分时次数
    if conf.getConfig("custom_dashboard", "firewall.fw_ip_pair_count_per_day") == "Y":
        try:
            doc_type = "fw_ip_pair_count_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            field = "gw_ip_pair"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
            day = datetime.now() - timedelta(2)
            fw_top_count_per_day(idx_name, doc_type, conn, day, localtz, field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计"IP对"在每天内被pass/deny的字节数的TOPN，并统计TOPN中的各"IP对"的24小时分时字节数
    if conf.getConfig("custom_dashboard", "firewall.fw_ip_pair_sum_bytes_per_day") == "Y":
        try:
            doc_type = "fw_ip_pair_sum_bytes_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            key_field = "gw_ip_pair"
            value_field = "gw_bytes"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            fw_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_field, size, base_condition)
            day = datetime.now() - timedelta(2)
            fw_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计"IP对"在每天内被pass/deny的数据包数的TOPN，并统计TOPN中的各"IP对"的24小时分时数据包数
    if conf.getConfig("custom_dashboard", "firewall.fw_ip_pair_sum_packets_per_day") == "Y":
        try:
            doc_type = "fw_ip_pair_sum_packets_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            # 分组的字段
            key_field = "gw_ip_pair"
            value_field = "gw_packets"
            # 取topN
            size = 100
            day = datetime.now() - timedelta(1)
            fw_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_field, size, base_condition)
            day = datetime.now() - timedelta(2)
            fw_top_sum_per_day(idx_name, doc_type, conn, day, localtz, key_field, value_field, size, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))

    # 统计国家、城市在每天内被pass/deny的次数的TOPN
    if conf.getConfig("custom_dashboard", "firewall.fw_geo_count_per_day") == "Y":
        try:
            doc_type = "fw_geo_count_per_day"
            # 删除过期的统计数据
            clear_index(idx_name, doc_type, conn, days)

            day = datetime.now() - timedelta(1)
            fw_geo_count_per_day(idx_name, doc_type, conn, day, localtz, base_condition)
            day = datetime.now() - timedelta(2)
            fw_geo_count_per_day(idx_name, doc_type, conn, day, localtz, base_condition)
        except Exception, e:
            sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))


if __name__ == "__main__":
    """
    测试
    """
    
    stats_firewall_dashboard()
