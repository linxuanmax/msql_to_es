#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-27"

import os
import fnmatch
import datetime
from smb import smb_structs
from nmb.NetBIOS import NetBIOS
from smb.SMBConnection import SMBConnection

def getBIOSName( remote_smb_ip, timeout=30 ):
    # 通过IP地址，查找smb服务器的名称
    srv_name = None
    
    bios = NetBIOS()
    try:
        srv_name = bios.queryIPForName( remote_smb_ip, timeout=timeout )
    except:
        print "查找samba服务器的名称时超时。"
    finally:
        bios.close()
        
    return srv_name


def incpattern( dname ):
    """
    判断字符串中是否包含通配符*或?
    """

    incp = True
    if dname.find('*') < 0 and dname.find('?') < 0:
        incp = False

    return incp


class FileSMB:
    smb = None

    def __init__( self, username, password, hostip, port = 139 ):
        """
        没有用户名和密码时，传入空字符串
        """

        srvname = getBIOSName( hostip )
        if srvname == None:
            return
        else:
            servername = srvname[0]

        if username == None:
            username = ''
        if password == None:
            password = ''
       
        smb_structs.SUPPORT_SMB2 = True
        self.smb = SMBConnection( username, password, 'smbfileclient', servername, use_ntlm_v2 = True)
        self.smb.connect( hostip, port=port )
        
    def __del__( self ):
        if self.smb != None:
            self.smb.close()
            self.smb = None

    def GetPatternList( self, pathlist, nextpattern, ispath = True ):
        """
        获取下一级匹配列表
        pathlist: 父路径列表
        nextpattern: 下级匹配模式
        """

        lst = []
        if len(pathlist) == 0:
            for cname in self.smb.listShares():
                if fnmatch.fnmatchcase( cname.name.lower(), nextpattern.lower() ):
                    lst.append( cname.name.lower() )
        else:
            for p in pathlist:
                plst = p.split( '/' )
                svrname = plst[0]
                pathname = "/".join( plst[1:] )
                if pathname == '':
                    pathname = '/'
                
                for cname in self.smb.listPath( svrname, pathname ):
                    if fnmatch.fnmatch( cname.filename.lower(), nextpattern.lower() ):
                        if (ispath == True and cname.isDirectory == True) or \
                                (ispath == False and cname.isDirectory == False):
                            lst.append( os.path.join( p, cname.filename.lower() ) )
        
        return lst


    def GetFileList( self, Pattern ):
        """
        根据文件、路径通配符，获取所有符合条件的文件列表
        Pattern必须为 path/filename 的形式
        其中 path 和 filename均可以包含有*和?通配符
        """

        lstFile = []
        pat = ()
        if Pattern.startswith( '/' ):
            pat = os.path.split( Pattern[1:] )
        else:
            pat = os.path.split( Pattern )
        
        fpath = pat[0]
        fname = pat[1]

        lst = fpath.split( '/' )
        for p in lst:
            if incpattern( p ):
                lstFile = self.GetPatternList( lstFile, p )
            elif len(lstFile) == 0:
                lstFile.append( p )
            else:
                for i in range( len( lstFile ) ):
                    lstFile[i] = os.path.join( lstFile[i], p )

        if incpattern( fname ):
            lstFile = self.GetPatternList( lstFile, fname, False )
        else:
            for i in range( len( lstFile ) ):
                lstFile[i] = os.path.join( lstFile[i], fname )

        return lstFile

    def DownLoadFile( self, LocalFile, RemoteFile ):
        #print 'Downloading file %s' % ( RemoteFile )
        lstFile = os.path.split(LocalFile)
        if os.path.isdir( lstFile[0] ) == False:
            os.makedirs( lstFile[0] )

        lst = RemoteFile.split('/')
        svrname = lst[0]
        path = '/'.join(lst[1:])

        f = open( LocalFile, 'w' )
        self.smb.retrieveFile( svrname, path, f )
        f.close()

        return True

    def DownLoadFilePattern( self, LocalDir, Pattern ):
        """
        Pattern必须为 path + filename 的形式
        其中 path 和 filename均可以包含有*和?通配符
        """

        if os.path.isdir( LocalDir ) == False:
            os.makedirs( LocalDir )

        lstFile = self.GetFileList( Pattern )

        for filename in lstFile:
            pat = os.path.split( filename )
            fname = os.path.join( LocalDir, pat[1] )

            lst = filename.split('/')
            svrname = lst[0]
            path = '/'.join(lst[1:])
            
            f = open( fname, 'w' )
            self.smb.retrieveFile( svrname, path, f)
            f.close()

        return True

    def getFileModiTime( self, filename ):
        """
        获取服务器上文件的最后修改时间
        获取失败的话，返回1970-01-01 00:00:00
        filename: 文件名，可带全路径
        """

        fileModiTime = datetime.datetime(1970, 1, 1)
        try:
            lst = filename.split('/')
            svrname = lst[0]
            pathname = '/'.join(lst[1: len(lst) -1])
            fname = lst[len(lst) -1]
            
            for cname in self.smb.listPath( svrname, pathname ):
                #print cname.filename
                #print cname.last_write_time
                if cname.filename.lower() == fname.lower():
                    fileModiTime = datetime.datetime.fromtimestamp(cname.last_write_time)
        except Exception, e:
            print "获取文件最后修改时间失败。", e

            return fileModiTime

        return fileModiTime

    def close( self ):
        if self.smb != None:
            self.smb.close()
            self.smb = None


# 测试用
if __name__ == '__main__':
    # 测试一
    samba = FileSMB('', '', '192.168.3.100')
    samba.DownLoadFilePattern( 'download', '/q*load/diskflt_2.0/app/*.cpp')
    samba.close()

    # 测试二
    samba = FileSMB('fallmoon', '123465', '192.168.2.110')
    lstFile = samba.GetFileList('q*load/diskflt_2.0/app/*.cpp')
    for filename in lstFile:
        pat = os.path.split(filename)
        localFile = os.path.join('download1', pat[1])
        samba.DownLoadFile(localFile, filename)
    samba.close()

