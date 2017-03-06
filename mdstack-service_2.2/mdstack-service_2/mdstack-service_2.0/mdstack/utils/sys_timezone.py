#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-11-14"

from pytz import timezone

def toLocalDatetime( dt ):
    """
    将其它时区的datetime转换成本地时区的datetime
    """

    localtz = timezone( 'Asia/Shanghai' )
    return localtz.localize( dt )


