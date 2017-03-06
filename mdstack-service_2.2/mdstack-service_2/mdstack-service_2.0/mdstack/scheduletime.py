#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-08-29"

import re
import datetime

def fmtSchedTime(schedTime):
    """
    格式化计划任务时间
    """

    schedWeeks = 0
    schedDays = 0
    schedHours = 0
    schedMinutes = 0
    schedSeconds = 0

    if schedTime != None:
        schedTime = schedTime.lower()
        if schedTime.endswith('weeks'):
            schedWeeks = int(schedTime.replace('weeks', ''))
        if schedTime.endswith('days'):
            schedDays = int(schedTime.replace('days', ''))
        if schedTime.endswith('hours'):
            schedHours = int(schedTime.replace('hours', ''))
        if schedTime.endswith('minutes'):
            schedMinutes = int(schedTime.replace('minutes', ''))
        if schedTime.endswith('seconds'):
            schedSeconds = int(schedTime.replace('seconds', ''))

    return (schedWeeks, schedDays, schedHours, schedMinutes, schedSeconds)

def fmtSchedCron(schedCron):
    """
    格式化计划任务cron
    """

    cronYear = None
    cronMonth = None
    cronWeek = None
    cronDayofWeek = None
    cronDay = None
    cronHour = None
    cronMinute = None
    cronSecond = None

    crons = schedCron.split('|')
    if crons[0] != '*':
        cronSecond = crons[0]
    if crons[1] != '*':
        cronMinute = crons[1]
    if crons[2] != '*':
        cronHour = crons[2]
    if crons[3] != '*':
        cronDay = crons[3]
    if crons[4] != '*':
        cronDayofWeek = crons[4]
    if crons[5] != '*':
        cronWeek = crons[5]
    if crons[6] != '*':
        cronMonth = crons[6]
    if crons[7] != '*':
        cronYear = crons[7]

    return (cronSecond, cronMinute, cronHour, cronDay, cronDayofWeek, cronWeek, cronMonth, cronYear)

def getBaseTime(curtime, snapto='second'):
    """
    利用当前时间，计算基准时间
    snapto: 对其到, 包括：second, minute, hour, day, month, year
            monday, tuesday, wednesday, thursday, friday, saturday, sunday
    curtime: 当前时间
    """
    
    basetime = curtime.replace(microsecond = 0) # 首先去掉毫秒
    if snapto == 'second':
        return basetime
    elif snapto == 'minute':
        return basetime.replace(second = 0)
    elif snapto == 'hour':
        return  basetime.replace(minute = 0, second = 0)
    elif snapto == 'day':
        return basetime.replace(hour = 0, minute = 0, second = 0)
    elif snapto == 'month':
        return basetime.replace(day = 1, hour = 0, minute = 0, second = 0)
    elif snapto == 'year':
        return basetime.replace(month = 1, day = 1, hour = 0, minute = 0, second = 0)
    else:
        curweekday = basetime.weekday()  # 0:星期一; 1:星期二;依此类推
        weekday = curweekday
        if snapto == 'monday':
            weekday = 0
        elif snapto == 'tuesday':
            weekday = 1
        elif snapto == 'wednesday':
            weekday = 2
        elif snapto == 'thursday':
            weekday = 3
        elif snapto == 'friday':
            weekday = 4
        elif snapto == 'saturday':
            weekday = 5
        elif snapto == 'sunday':
            weekday = 6
        
        if curweekday >= weekday:
            days = curweekday - weekday
        else:
            days = curweekday + 7 - weekday

        return basetime.replace(hour = 0, minute = 0, second = 0) - datetime.timedelta(days)

    return basetime

def getTimeDelta(interval):
    """
    利用时间间隔字符串，生成时间间隔
    注意：对于months和years，python不好计算时间间隔
    """

    if interval.endswith('seconds'):
        timedelta = datetime.timedelta(seconds = int(interval[:len(interval) - 7]))
    elif interval.endswith('minutes'):
        timedelta = datetime.timedelta(minutes = int(interval[:len(interval) - 7]))
    elif interval.endswith('hours'):
        timedelta = datetime.timedelta(hours = int(interval[:len(interval) - 5]))
    elif interval.endswith('days'):
        timedelta = datetime.timedelta(days = int(interval[:len(interval) - 4]))
    elif interval.endswith('weeks'):
        timedelta = datetime.timedelta(weeks = int(interval[:len(interval) - 5]))

    return timedelta

def dateAdd(datepart, number, date):
    """
    datepart: 可以为year(年),month(月),day(日)
    number: 可为正、负整数
    date: datetime.date
    返回datetime.date
    """
    
    datepart = datepart.lower()
    if datepart == 'day':
        return date + datetime.timedelta(days = number)
    else:
        y = date.year
        m = date.month
        d = date.day
        r = None
        if datepart == 'month':
            y += (m + number) / 12
            if y > datetime.MAXYEAR:
                y = datetime.MAXYEAR
            if y < datetime.MINYEAR:
                y = datetime.MINYEAR
            m = (m + number) % 12
            while r == None:
                try:
                    r = datetime.date(y, m, d)
                except:
                    d -= 1
        elif datepart == 'year':
            y += number
            if y > datetime.MAXYEAR:
                y = datetime.MAXYEAR
            if y < datetime.MINYEAR:
                y = datetime.MINYEAR
            try:
                r = datetime.date(y, m, d)
            except:  # 闰年 -> 平年
                r = datetime.date(y, m, d - 1)
        return r

def fmtRelTime(relTime, curTime = datetime.datetime.now()):
    """
    将相对时间转换为绝对时间
    relTime: string
    curTime：datetime.datetime系统当前时间
    """

    rtime = curTime
    relTime = relTime.lstrip().rstrip().lower()
    # 0代表的时间是：1970-01-01 00:00:00
    if relTime == '0':
        return datetime.datetime(1970, 1, 1)
    # 关键字now代表当前时间 
    if relTime == 'now':
        return rtime.replace(microsecond = 0)

    # 判断relTime是相对还是绝对时间
    if relTime.startswith('+') or relTime.startswith('-'):
        # 相对时间
        foretime = '-15minutes'
        backtime = 'second'
        lst = relTime.split('@')
        if len(lst) == 1:
            # 省略了@second
            foretime = lst[0]
        else:
            foretime = lst[0]
            backtime = lst[1]

        # 获取基准时间
        basetime = getBaseTime(curTime, backtime)
        # 计算相对时间
        if foretime.endswith('months'):
            rdate = dateAdd('month', int(foretime[:len(foretime) - 6]), basetime.date())
            rtime = datetime.datetime.combine(rdate, basetime.time())
        elif foretime.endswith('years'):
            rdate = dateAdd('year', int(foretime[:len(foretime) - 5]), basetime.date())
            rtime = datetime.datetime.combine(rdate, basetime.time())
        else:
            rtime = basetime + getTimeDelta(foretime)
    else:
        # 绝对时间 （考虑了只有年月日的情况）
        rep = re.compile(r'(\d{4})-(\d{1,2})-(\d{1,2})(\s(\d{1,2}):(\d{1,2}):(\d{1,2}))?')
        m = rep.match(relTime)
        if m != None:
            hour = 0
            minute = 0
            second = 0
            if m.group(4) != None:
                hour = int(m.group(5))
                minute = int(m.group(6))
                second = int(m.group(7))
            rtime = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), \
                    hour, minute, second)    
                    
            # rtime = datetime.datetime.strptime(relTime, "%Y-%m-%d %H:%M:%S")

    return rtime
