# -*- coding=utf-8 -*-

import pyes
from pyes import *
from pytz import timezone
from datetime import *
import os
import traceback
from mdstack.utils import sys_config, sys_log

def term_facet(conn, idx_name, idx_type=None, string_query={"query":"*", "default_field":"_all", "default_operator":"AND"}, \
                range_filter={"range_field":"@timestamp", "from_value":"", "to_value":"", "include_upper":True, "include_lower":True}, \
                term_facet={"name":"results", "field":"_all", "size":50, "order":None}):
    # QueryStringQuery查询
    qry = pyes.query.QueryStringQuery(string_query["query"], default_field = string_query["default_field"], default_operator = string_query["default_operator"])
    # RangeFilter过滤
    ftr = pyes.filters.RangeFilter(pyes.utils.ESRange(range_filter["range_field"], \
            from_value=range_filter["from_value"], to_value=range_filter["to_value"], \
            include_upper=range_filter["include_upper"], include_lower=range_filter["include_lower"]))
    # FilteredQuery查询
    query = pyes.query.FilteredQuery(qry, ftr)
    # Term Facet
    facet = pyes.facets.TermFacet(name = term_facet["name"], field = term_facet["field"], size = term_facet["size"], order = term_facet["order"])
    # Search
    search = query.search()
    search.facet.add(facet)
    rs = conn.search(search, indices = [idx_name])
    return rs

def statistical(conn, idx_name, idx_type=None, string_query={"query":"*", "default_field":"_all", "default_operator":"AND"}, \
                range_filter={"range_field":"@timestamp", "from_value":"", "to_value":"", "include_upper":True, "include_lower":True}, \
                statistical={"name":"results", "field":""}):
    # QueryStringQuery查询
    qry = pyes.query.QueryStringQuery(string_query["query"], default_field = string_query["default_field"], default_operator = string_query["default_operator"])
    # RangeFilter过滤
    ftr = pyes.filters.RangeFilter(pyes.utils.ESRange(range_filter["range_field"], \
            from_value=range_filter["from_value"], to_value=range_filter["to_value"], \
            include_upper=range_filter["include_upper"], include_lower=range_filter["include_lower"]))
    # FilteredQuery查询
    query = pyes.query.FilteredQuery(qry, ftr)
    # Statistical Facet
    facet = pyes.facets.StatisticalFacet(name = statistical["name"], field = statistical["field"])
    # Search
    search = query.search()
    search.facet.add(facet)
    rs = conn.search(search, indices = [idx_name])
    return rs

def term_stats(conn, idx_name, idx_type=None, string_query={"query":"*", "default_field":"_all", "default_operator":"AND"}, \
                range_filter={"range_field":"@timestamp", "from_value":"", "to_value":"", "include_upper":True, "include_lower":True}, \
                term_stats={"name":"results", "key_field":None, "value_field":None, "size":50, "order":None}):
    # QueryStringQuery查询
    qry = pyes.query.QueryStringQuery(string_query["query"], default_field = string_query["default_field"], default_operator = string_query["default_operator"])
    # RangeFilter过滤
    ftr = pyes.filters.RangeFilter(pyes.utils.ESRange(range_filter["range_field"], \
            from_value=range_filter["from_value"], to_value=range_filter["to_value"], \
            include_upper=range_filter["include_upper"], include_lower=range_filter["include_lower"]))
    # FilteredQuery查询
    query = pyes.query.FilteredQuery(qry, ftr)
    # Term Stats Facet
    facet = pyes.facets.TermStatsFacet(name = term_stats["name"], size = term_stats["size"], order = term_stats["order"], key_field = term_stats["key_field"], value_field = term_stats["value_field"])
    # Search
    search = query.search()
    search.facet.add(facet)
    rs = conn.search(search, indices = [idx_name])
    return rs
