# -*- coding: utf-8 -*-
"""
Created  8 22  2018
@author: 李宁
"""

import os

def start():
    os.system('net start "ArcGIS Server"')

def stop():
    os.system('net stop "ArcGIS Server"')
