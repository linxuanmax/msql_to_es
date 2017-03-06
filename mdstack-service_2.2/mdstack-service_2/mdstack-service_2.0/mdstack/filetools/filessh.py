#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-24"

import os
import datetime
import paramiko
import fnmatch
from stat import S_ISDIR

def incpattern(dname):
    """
    判断字符串中是否包含通配符*或?
    """

    incp = True
    if dname.find('*') < 0 and dname.find('?') < 0:
        incp = False

    return incp

class FileSSH:
    trans = None
    sftp = None

    def __init__( self, userdic, host, port = 22 ):
        """
        userdic 为连接远程服务器用户信息字典
        必须包含的keys有：username, password；
        可能包含的keys有：rsa_private_key(私钥文件名)
        """
        
        username = userdic['username']
        password = userdic['password']
        try:
            self.trans = paramiko.Transport( (host, port) )
        except Exception, e:
            print "初始化远程端口失败。", e
            return

        if userdic.has_key( 'rsa_private_key' ):
            rsa_key = userdic['rsa_private_key']
            if password != None and len( password ) == 0:
                password = None
            
            try:
                self.trans.start_client()
                ki = paramiko.RSAKey.from_private_key_file( rsa_key, password=password )
            except Exception, e:
                print "加载密钥文件失败--", rsa_key, e
                return

            agent = paramiko.Agent()
            agent_keys = agent.get_keys() + (ki,)
            if len(agent_keys) > 0:
                for key in agent_keys:
                    print "Trying ssh-agent key %s" % key.get_fingerprint().encode('hex')
                    try:
                        self.trans.auth_publickey( username, key )
                        print "... success!"
                        break
                    except Exception, e:
                        print "... failed!", e
        
            try:
                if self.trans.is_authenticated():
                    self.sftp = self.trans.open_session()
                    self.sftp = paramiko.SFTPClient.from_transport( self.trans )
            except Exception, e:
                print "Failed to connect to host %s using user name and private key. " %host, e
        else:
            try:
                hostkey = self._getHostKey( host )
                self.trans.connect( username = username, password = password, hostkey = hostkey )
                self.sftp = paramiko.SFTPClient.from_transport( self.trans )
            except Exception, e:
                print "Failed to connect to host %s using user name and password. " % host, e

    def __del__( self ):
        if self.sftp != None:
            self.sftp.close()
            self.sftp = None

        if self.trans != None:
            self.trans.close()
            self.trans = None

    def GetPatternList( self, pathlist, nextpattern, ispath = True ):
        """
        获取下一级匹配列表
        pathlist: 父路径列表
        nextpattern: 下级匹配模式
        """

        lst = []
        curPath = self.sftp.getcwd()

        for p in pathlist:
            self.sftp.chdir( p )
            for cname in self.sftp.listdir():
                #print '1.cname: %s' %( cname )
                if fnmatch.fnmatch( cname, nextpattern ):
                    #print '2.cname: %s' %( cname )
                    if (ispath == True and self.isDir( cname )) or \
                            (ispath == False and not self.isDir( cname )):
                        lst.append( os.path.join( p, cname ) )
            self.sftp.chdir( curPath )

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
            lstFile = ['/']
            self.sftp.chdir( '/' )
            pat = os.path.split( Pattern[1:] )
        else:
            lstFile = ['']
            self.sftp.chdir( None )
            pat = os.path.split( Pattern )
        fpath = pat[0]
        fname = pat[1]

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

    def DownLoadFile( self, LocalFile, RemoteFile ):
        #print 'Downloading file %s' % ( RemoteFile )

        lstFile = os.path.split(LocalFile)
        if os.path.isdir( lstFile[0] ) == False:
            os.makedirs( lstFile[0] )
        
        self.sftp.get( RemoteFile, LocalFile )

        return True

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
            self.sftp.get( f, fname )

        return True

    def _getHostKey( self, hostname ):
        # 检索本机是否已经有hostkey
        hostkey = None

        host_keys = {}
        try:
            host_keys = paramiko.util.load_host_keys( os.path.expanduser('~/.ssh/known_hosts') )
        except IOError:
            host_keys = {}

        if host_keys.has_key( hostname ):
            hostkeytype = host_keys[hostname].keys()[0]
            hostkey = host_keys[hostname][hostkeytype]

        return hostkey

    def isDir( self, path ):
        try:
            return S_ISDIR(self.sftp.stat(path).st_mode)
        except IOError:
            return False

    def getFileModiTime( self, filename ):
        """
        获取服务器上文件的最后修改时间
        获取失败的话，返回1970-01-01 00:00:00
        filename: 文件名，可带全路径
        """

        fileModiTime = datetime.datetime(1970, 1, 1)
        try:
            res = self.sftp.stat(filename).st_mtime
            fileModiTime = datetime.datetime.fromtimestamp(res)
        except Exception, e:
            print "获取文件最后修改时间失败。", e

            return fileModiTime

        return fileModiTime

    def close( self ):
        if self.sftp != None:
            self.sftp.close()
            self.sftp = None

        if self.trans != None:
            self.trans.close()
            self.trans = None

# 测试
if __name__ == '__main__':
    # 测试一
    ssh = FileSSH({'username':'user', 'password':'123465'}, '192.168.1.23', 22)
    ssh.DownLoadFilePattern( 'download1', 'build/K*/*.rb')
    ssh.close()

    # 测试二
    ssh = FileSSH({'username':'user', 'password':'123465'}, '192.168.1.23', 22)
    lstFile = ssh.GetFileList ( '/lib/x*/lib*.so')
    for filename in lstFile:
        pat = os.path.split(filename)
        localFile = os.path.join('download', pat[1])
        ssh.DownLoadFile(localFile, filename)
    ssh.close()

