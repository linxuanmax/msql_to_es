#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-22"

import os
import sys
import datetime
import ftplib
import fnmatch
from ctypes import *

def incpattern(dname):
    """
    判断字符串中是否包含通配符*或? 
    """

    incp = True
    if dname.find('*') < 0 and dname.find('?') < 0:
        incp = False

    return incp

class FileFtp:
    ftp = None
    bIsDir = False
    path = ""
    
    def __init__( self, username, password, host, port = 21 ):
        self.ftp = ftplib.FTP()
        #self.ftp.set_debuglevel(2)     #打开调试级别2，显示详细信息
        #self.ftp.set_pasv(0)           #0主动模式 1 #被动模式
        self.ftp.connect( host, str(port) )
        self.ftp.login( username, password )
        #print self.ftp.welcome

    def __del__( self ):
        if self.ftp != None:
            self.ftp.close()
            self.ftp = None

    #def Login( self, user, passwd ):
    #    self.ftp.login( user, passwd )
    #    print self.ftp.welcome

    def DownLoadFile( self, LocalFile, RemoteFile ):
        #print 'Downloading file %s' % ( RemoteFile )

        lstFile = os.path.split(LocalFile)
        if os.path.isdir( lstFile[0] ) == False:
            os.makedirs( lstFile[0] )

        file_handler = open( LocalFile, 'wb' )
        self.ftp.retrbinary( "RETR %s" %( RemoteFile ), file_handler.write )
        file_handler.close()

        return True

    def DownLoadFileTree( self, LocalDir, RemoteDir ):
        if os.path.isdir( LocalDir ) == False:
            os.makedirs( LocalDir )
              
        self.ftp.cwd( RemoteDir )
        RemoteNames = self.ftp.nlst()
        for file in RemoteNames:
            Local = os.path.join( LocalDir, file )
            if self.isDir( file ):
                self.DownLoadFileTree( Local, file )               
            else:
                self.DownLoadFile( Local, file )
        
        self.ftp.cwd( ".." )
        return
    
    def GetPatternList( self, pathlist, nextpattern, ispath = True ):
        """
        获取下一级匹配列表
        pathlist: 父路径列表
        nextpattern: 下级匹配模式
        """
        
        lst = []
        curPath = self.ftp.pwd()

        for p in pathlist:
            self.ftp.cwd( p )
            cnames = self.ftp.nlst()
            for cname in cnames:
                if fnmatch.fnmatch( cname, nextpattern ):
                    if (ispath == True and self.isDir( cname )) or \
                            (ispath == False and not self.isDir( cname )):
                        lst.append( os.path.join( p, cname ) )
            self.ftp.cwd( curPath )
            
        return lst

    def GetFileList( self, Pattern ):
        """
        根据文件、路径通配符，获取所有符合条件的文件列表
        Pattern必须为 path/filename 的形式
        其中 path 和 filename均可以包含有*和?通配符
        """
        
        lstFile = ['/']
        pat = ()
        if Pattern.startswith( '/' ):
            pat = os.path.split( Pattern[1:] )
        else:
            pat = os.path.split( Pattern )
        fpath = pat[0]
        fname = pat[1]

        self.ftp.cwd( '/' )

        lst = fpath.split( '/' )
        for p in lst:
            if incpattern( p ):
                lstFile = self.GetPatternList( lstFile, p )
            else:
                for i in range( len( lstFile ) ):
                    lstFile[i] = os.path.join( lstFile[i], p )

        if incpattern( fname ):
            lstFile = self.GetPatternList( lstFile, fname, False )
        else:
            for i in range( len( lstFile ) ):
                lstFile[i] = os.path.join( lstFile[i], fname )

        return lstFile

    def DownLoadFilePattern( self, LocalDir, Pattern ):
        """
        Pattern必须为 path + filename 的形式
        其中 path 和 filename均可以包含有*和?通配符
        """

        if os.path.isdir( LocalDir ) == False:
            os.makedirs( LocalDir )

        lstFile = self.GetFileList( Pattern )

        for f in lstFile:
            pat = os.path.split( f )
            fname = os.path.join(LocalDir, pat[1])
            self.DownLoadFile( fname, f)
            
        return True

    def UpLoadFile( self, LocalFile, RemoteFile ):
        if os.path.isfile( LocalFile ) == False:
            return False
        file_handler = open( LocalFile, "rb" )
        self.ftp.storbinary( 'STOR %s'%RemoteFile, file_handler, 4096 )
        file_handler.close()

        return True

    def UpLoadFileTree( self, LocalDir, RemoteDir ):
        if os.path.isdir( LocalDir ) == False:
            return False
        
        LocalNames = os.listdir( LocalDir )
        print RemoteDir
        self.ftp.cwd( RemoteDir )
        for Local in LocalNames:
            src = os.path.join( LocalDir, Local)
            if os.path.isdir( src ):
                self.UpLoadFileTree( src, Local )
            else:
                self.UpLoadFile( src, Local )
             
        self.ftp.cwd( ".." )
        return

    def show( self, list ):
        result = list.lower().split( " " )
        #print "result: %s" %(result)
        #if self.path in result and "<dir>" in result:
        if result[len(result)-1].lower() == self.path.lower() and result[0].startswith('d'):
            self.bIsDir = True
                                              
    def isDir( self, path ):
        self.bIsDir = False
        self.path = path
        #this uses callback function ,that will change bIsDir value
        self.ftp.retrlines( 'LIST', self.show )
        return self.bIsDir
                                                                                               
    def getFileModiTime( self, filename ):
        """
        获取服务器上文件的最后修改时间
        获取失败的话，返回1970-01-01 00:00:00
        filename: 文件名，可带全路径
        """

        fileModiTime = datetime.datetime(1970, 1, 1)
        cmd = "MDTM " + filename
        try:
            res = self.ftp.sendcmd( cmd )
            res = res.split(' ')[1]
            fileModiTime = datetime.datetime(int(res[:4]), int(res[4:6]), int(res[6:8]), \
                    int(res[8:10]), int(res[10:12]), int(res[12:14]))
        except Exception, e:
            print "获取文件最后修改时间失败。", e

            return fileModiTime

        return fileModiTime

    def close( self ):
        self.ftp.quit()


# 测试用
if __name__ == '__main__':
    # 测试一
    ftp = FileFtp('test', '123465', '192.168.3.100')
    ftp.DownLoadFilePattern('download', '/demo/j*/S*/*.jar')
    ftp.close()

    """
    # 测试二
    ftp = FileFtp('test', '123465', '192.168.2.110')
    lstFile = ftp.GetFileList('demo/j*/S*/*.jar')
    for filename in lstFile:
        pat = os.path.split(filename)
        localFile = os.path.join('download1', pat[1])
        ftp.DownLoadFile(localFile, filename)
    ftp.close()
    
    # 测试三
    ftp = FileFtp('', '', 'ftp.pku.edu.cn')
    #ftp.DownLoadFilePattern('download', 'open/h*/*.bz2')
    print ftp.getFileModiTime('/open/hwloc/hwloc-1.4.1.tar.bz2')
    ftp.close()
    """

