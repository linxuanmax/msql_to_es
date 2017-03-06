#!/opt/secsphere/bin/python
# -*- coding: utf-8 -*-

import os.path
import os
import stat
import codecs
from glob import glob

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES 

import mdstack

packages, data_files = [], []
root_dir = os.path.dirname(__file__)

if root_dir != '':
    os.chdir(root_dir)

if os.path.exists("README"):
    long_description = codecs.open('README', "r", "utf-8").read()
else:
    long_description = ""

src_dir = "mdstack"

conf_target = '/opt/mdstack/conf/mdstackd'
conf_file = filter(os.path.isfile,glob(src_dir + '/conf/*.conf'))

auto_target = '/etc/init.d'
auto_file = filter(os.path.isfile,glob(src_dir + '/mdstackd'))

additional_files = [(conf_target,conf_file),(auto_target,auto_file)]

setup(
	name = "mdstackd",
	version=mdstack.__version__,
	description=mdstack.__doc__,
	author=mdstack.__author__,
	author_email=mdstack.__contact__,
	url=mdstack.__homepage__,
	platforms=["any"],
	packages=find_packages(),
	data_files=additional_files,
	zip_safe=False,
	scripts=["bin/run_datastats.py", "bin/run_taskscheduler.py", "bin/run_dataextraction.py", "bin/run_pullfiles.py", \
               "bin/datastats_service.py", "bin/taskscheduler_service.py", "bin/dataextraction_service.py", "bin/pullfiles_service.py"],
	classifiers=[
	"Development Status :: 2 - Alpha",
	"Operating System :: OS Independent",
	"Programming Language :: Python",
	"License :: OSI Approved :: GPL License",
	"Intended Audience :: Developers",
	],
	long_description=long_description,
)


if os.path.exists(conf_target + '/mdstack.conf'):
    os.chmod(conf_target + '/mdstack.conf', stat.S_IWRITE|stat.S_IREAD|stat.S_IWGRP|stat.S_IRGRP|stat.S_IWOTH|stat.S_IROTH)

if os.path.exists(auto_target + '/mdstackd'):
    os.chmod(auto_target + '/mdstackd', stat.S_IEXEC|stat.S_IWRITE|stat.S_IREAD|stat.S_IXGRP|stat.S_IRGRP|stat.S_IXOTH|stat.S_IROTH)
    os.popen('sysv-rc-conf mdstackd on')

