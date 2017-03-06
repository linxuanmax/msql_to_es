# coding=utf-8

import json
import os
import time
import datetime


class Location(object):

    def __init__(self, lst, taskname, mianincr, incType):
        self._lst = lst
        self._taskname = taskname
        self._mianincr = mianincr
        self._inctype = incType

    def up_location(self):

        if self._inctype == 2:
            filed = os.path.split(os.path.realpath(__file__))[0] + '/track/' + "%s" % (self._taskname + '.json')
            load_f = open(filed, 'r')
            files = json.load(load_f)

            if self._mianincr == 'number':
                files['curpos'] = 0
            elif self._mianincr == 'string':
                files['curpos'] = ' '
            elif self._mianincr == 'time':
                files['curpos'] = '1980-01-01 00:00:00'
            load_f.close()
            curpos_stime = self._lst[-1][-1]
            curpos_stime = curpos_stime.strftime("%Y-%m-%d %H:%M:%S")
            t1 = time.strptime(curpos_stime, "%Y-%m-%d %H:%M:%S")
            timeStamp = int(time.mktime(t1))
            t2 = timeStamp + 3600
            t3 = time.localtime(t2)
            curpos_etime = time.strftime("%Y-%m-%d %H:%M:%S", t3)


            dump_f = open(filed, 'w')
            files['curpos_stime'] = curpos_stime
            files['curpos_etime'] = curpos_etime
            json.dump(files, dump_f)
            dump_f.close()
        else:
            filed = os.path.split(os.path.realpath(__file__))[0] + '/track/' + "%s" % (self._taskname + '.json')
            load_f = open(filed, 'r')
            files = json.load(load_f)
            dump_f = open(filed, 'w')

            curpos = self._lst[-1][0]
            files['curpos'] = curpos
            json.dump(files, dump_f)
            dump_f.close()

    def add_location(self):

        if self._inctype == 2:
            filed = os.path.split(os.path.realpath(__file__))[0] + '/track/' + "%s" % (self._taskname + '.json')
            load_f = open(filed, 'r')
            files = json.load(load_f)

            if self._mianincr == 'number':
                files['curpos'] = 0
            elif self._mianincr == 'string':
                files['curpos'] = ' '
            elif self._mianincr == 'time':
                files['curpos'] = '1980-01-01 00:00:00'
            load_f.close()

            curpos_stime = files['curpos_etime'].encode('utf-8')
            str_curpos_stime = datetime.datetime.strptime(curpos_stime, '%Y-%m-%d %H:%M:%S')
            date_now = datetime.datetime.now()
            if str_curpos_stime <= date_now:
                t1 = time.strptime(curpos_stime, "%Y-%m-%d %H:%M:%S")
                time_stamp = int(time.mktime(t1))
                t2 = time_stamp + 86400*2
                t3 = time.localtime(t2)
                curpos_etime = time.strftime("%Y-%m-%d %H:%M:%S", t3)

                dump_f = open(filed, 'w')
                files['curpos_stime'] = curpos_stime
                files['curpos_etime'] = curpos_etime
                json.dump(files, dump_f)
                dump_f.close()
            else:
                time.sleep(5)



