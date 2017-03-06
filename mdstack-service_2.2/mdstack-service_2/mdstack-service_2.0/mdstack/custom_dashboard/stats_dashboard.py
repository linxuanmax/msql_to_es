# -*- coding=utf-8 -*-

import firewall_dashboard
import flows_dashboard
import traceback
from mdstack.utils import sys_log, sys_config

def stats_dashboard(pd):
    """
    统一调用入口函数
    """

    try:
        if pd != None and pd.is_master() == False:
            return

        # 自定义防火墙dashboard统计
        firewall_dashboard.stats_firewall_dashboard()
        flows_dashboard.stats_flows_dashboard()
    except Exception, e:
        # 配置文件
        configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
        conf = sys_config.SysConfig(configFile)
        # 错误日志
        logFile = conf.getConfig( "datastats", "logFile" )
        instance = conf.getConfig( "datastats", "instanceName" )
        sys_log.SysLog(logFile, instance).writeLog("error", str(traceback.format_exc()))
    
if __name__ == "__main__":
    """
    测试
    """
    
    stats_dashboard()