# -*- coding: utf-8 -*-
import d2f

dbpath = 'E:/ahdata/gdbserver/slpc.gdb'
cfgpath = 'E:/ahdata/config000.xlsx'
orclurl = 'oracle://ahslv2:ahsl@10.4.148.144:1521/oracle'

d2f.init(dbpath,cfgpath,orclurl)
d2f.do_update()
