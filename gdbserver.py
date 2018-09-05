# -*- coding: utf-8 -*-
# _*_ 在生成的文件夹中加入site-packages _*_
from site import addsitedir
from sys import executable
from os import path

interpreter = executable
sitepkg = path.dirname(interpreter) + "\\site-packages"
addsitedir(sitepkg)
# _*_ 在生成的文件夹中加入site-packages _*_
from sqlalchemy import *
import web
import getopt
import sys
import threading

#自己编写
import gisserver
import d2f

# 读取传入参数
dbpath = ''
cfgpath = ''
orclurl = ''

argv = sys.argv
if ('-h' in argv or '--help' in argv):
    print u'''
   -h/--help    打印帮助信息
   --gdb        指定gdb的绝对路径,必须
   --config     指定Classcode与图层名对照表,必须
   --orcl       指定oracle的URL,必须'''
    exit()

try:
    opts, args = getopt.getopt(argv[2:],'', ["gdb=", "config=", "orcl="]);

    for o,v in opts:
        #print o +'=' +v
        if o == '--gdb':
            dbpath = v
        if o == '--config':
            cfgpath = v
        if o == '--orcl':
            orclurl = v
except getopt.GetoptError:
    print("getopt error!");
    exit()


urls = (
    '/(.*)', 'index'
)


class index:
    lock = threading.Lock()             #用户锁

    def GET(self,param):
        #print 'updating=',param.find('update')
        if param.find('update') >= 0:
            try:
                index.lock.acquire()        #用户锁定，锁定期间其他用户不能进行更新操作
                gisserver.stop()
                print 'gis server is stopped'

                # 这里是更新任务
                self.do_update()

                gisserver.start()
                print 'gis server is started'
                index.lock.release()
            except IOError:
                print "0"
            else:
                return "1"

        if param.find('start') >= 0:
            gisserver.start()
            return 'ArcGIS server is started'

        if param.find('stop') >= 0:
            gisserver.stop()
            return 'ArcGIS server is stopped'

        if param.find('file') >= 0:
            myfile = ''
            db_engine = create_engine('oracle://ahslv2:ahsl@10.4.148.144:1521/oracle', echo=False)
            meta = MetaData()
            fT = Table('SketchFeature', meta, autoload=True, autoload_with=db_engine)

            myfile =  fT.c.keys()

            conn = db_engine.connect()
            s_insert = select([fT.c.ClassCode, fT.c.EntityCode, fT.c.ShapeType, fT.c.ShapeWKT])
            features = conn.execute(s_insert)
            for feature in features:
                myfile += feature[fT.c.ClassCode], feature[fT.c.EntityCode], feature[fT.c.ShapeType], feature[fT.c.ShapeWKT]
            return myfile

        return "如果要更新数据库，请输入'update'命令！"

    def do_update(self):
        global dbpath, cfgpath, orclurl

        print 'dbpath=', dbpath
        print 'cfgpath=', cfgpath
        print 'orclurl=', orclurl

        d2f.init(dbpath, cfgpath, orclurl)
        d2f.do_update()
        pass

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()