#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-08-23"

import os
import sys
import ConfigParser

def isFrozen():
    import imp
    return (
        hasattr(sys, "frozen") or
        imp.is_frozen("__main__"))

def getDir():
    if isFrozen():
        return os.path.abspath(os.path.dirname(sys.executable))
    return os.path.abspath(os.path.dirname(sys.argv[0]))

class SysConfig:
    """
    get/set config settings from config file
    """

    def __init__(self, configFileName):
        """
        Constructor
        """
        self.configFileName = configFileName

    def getConfig(self, section, key):
        keyValue = None
    
        """
        file = open(self.configFileName, "r")
        for line in file.readlines():
            lst = line.rstrip().split("=")
            if lst[0] == key:
                keyValue = lst[1]
        """
        try:
            if os.path.exists(self.configFileName):
                config = ConfigParser.ConfigParser()
                config.read(self.configFileName)
                keyValue = config.get(section, key)
        except Exception, e:
            print e
            
        return keyValue    
