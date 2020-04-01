# -*- coding:utf-8 -*-
import os
import configparser
from bomber.utils import env_detect

cur_env = env_detect().lower()
#  实例化configParser对象
config_obj = configparser.ConfigParser()
base_dir = os.path.dirname(__file__)
base_file_path = os.path.join(base_dir, "config/base.ini")
file_path = os.path.join(base_dir, "config/{}.ini".format(cur_env))
config_obj.read(base_file_path, encoding='utf-8')
config_obj.read(file_path, encoding='utf-8')
