# -*- coding: utf-8 -*-
from sqlalchemy import *
from sqlalchemy.sql import select
from sqlalchemy.schema import *
import arcpy
import xlrd
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#**********本模块全局变量.**************
dbpath = None
cfgpath = None
orcl_url = None

cfg_xls = None
cfg_sheet = None  # 通过索引顺序获取
nrows = None


# 所有图层名
lyrnames = None
# print lyrnames

def select_layer_name(classcode, shapetype):
    # global lyrnames
    index = -1
    for i in xrange(1,nrows):
        row_str =  str(cfg_sheet.row_values(i)).upper()
        # print row_str

        if row_str.find(classcode)> 0 and row_str.find(shapetype)> 0 :
            index = i
            break
    return lyrnames[i-1]


def do_update():
    print 'env.w  ',arcpy.env.workspace
    # 读取待变更feature数据
    db_engine = create_engine(orcl_url,echo=False)
    meta = MetaData()

    fT = Table('SketchFeature', meta, autoload=True, autoload_with=db_engine)
    eT = Table('Entity', meta, autoload=True, autoload_with=db_engine)

    #print fT.c.keys()

    conn = db_engine.connect()

    s_insert = select([fT.c.ClassCode, fT.c.EntityCode, fT.c.ShapeType, fT.c.ShapeWKT, eT.c.Name,eT.c.LabelPoint_X,
                       eT.c.LabelPoint_Y]).where(and_(fT.c.EntityCode == eT.c.Code, fT.c.State == 1))
    s_update = select([fT.c.ClassCode, fT.c.EntityCode, fT.c.ShapeType, fT.c.ShapeWKT, eT.c.Name,eT.c.LabelPoint_X,
                       eT.c.LabelPoint_Y]).where(and_(fT.c.EntityCode == eT.c.Code, fT.c.State == 2))
    s_delete = select([fT.c.ClassCode, fT.c.EntityCode, fT.c.ShapeType, fT.c.ShapeWKT, eT.c.Name,eT.c.LabelPoint_X,
                       eT.c.LabelPoint_Y]).where(and_(fT.c.EntityCode == eT.c.Code, fT.c.State == 3))

    fnum = 0
    #处理插入feature
    features = conn.execute(s_insert)

    for feature in features:
        # print feature[fT.c.ClassCode], feature[fT.c.ShapeType]
        layername = select_layer_name(feature[fT.c.ClassCode], feature[fT.c.ShapeType])

        # fnum = fnum + 1
        # print 'lyrname ', layername, fnum
        #

        fields = ['YSDM','CODE', 'NAME', 'X', 'Y', 'SHAPE@WKT']
        cursor =  arcpy.da.InsertCursor(os.path.join(arcpy.env.workspace ,layername), fields)
        cursor.insertRow((feature[fT.c.ClassCode],feature[fT.c.EntityCode],feature[eT.c.Name], feature[eT.c.LabelPoint_X],feature[eT.c.LabelPoint_Y],feature[fT.c.ShapeWKT]))
        del cursor

    print u'插入要素完成！'

    #处理修改feature
    fnum = 0
    features = conn.execute(s_update)

    for feature in features:
        # print feature[fT.c.ClassCode], feature[fT.c.ShapeType]
        layername = select_layer_name(feature[fT.c.ClassCode], feature[fT.c.ShapeType])
        # fnum = fnum + 1
        # print 'lyrname ', layername

        fields = ['NAME']
        with arcpy.da.UpdateCursor(os.path.join(arcpy.env.workspace, layername), fields,
                                   "CODE='" + feature[fT.c.EntityCode] + "'") as cursor:
            for row in cursor:
                cursor.deleteRow()
                pass

        fields = ['YSDM', 'CODE', 'NAME', 'X', 'Y', 'SHAPE@WKT']
        cursor = arcpy.da.InsertCursor(os.path.join(arcpy.env.workspace, layername), fields)
        cursor.insertRow((feature[fT.c.ClassCode], feature[fT.c.EntityCode], feature[eT.c.Name],
                          feature[eT.c.LabelPoint_X], feature[eT.c.LabelPoint_Y], feature[fT.c.ShapeWKT]))
        del cursor
        # print cursor.next()
    print u'修改要素完成'
    #处理删除feature
    fnum = 0
    features = conn.execute(s_delete)
    ft_update = fT.update()
    for feature in features:
        # print feature[fT.c.ClassCode], feature[fT.c.ShapeType]
        layername = select_layer_name(feature[fT.c.ClassCode], feature[fT.c.ShapeType])

        # fnum = fnum + 1
        # print 'lyrname ', layername

        fields = ['NAME']
        with arcpy.da.UpdateCursor(os.path.join(arcpy.env.workspace, layername), fields,
                                   "CODE='" + feature[fT.c.EntityCode] + "'") as cursor:
            for row in cursor:
                cursor.deleteRow()
                conn.execute(fT.update().values(isdelete=1).where(fT.c.EntityCode == feature[fT.c.EntityCode]))
                pass
    print u'删除要素完成'
    conn.close()

def init(db,cfg,orcl):
    global dbpath, cfgpath, cfg_sheet, cfg_xls, nrows, lyrnames, orcl_url

    dbpath = db
    cfgpath = cfg
    orcl_url = orcl

    print 'dbpath=', dbpath
    print 'cfgpath=', cfgpath
    print 'orclurl=', orcl_url

    # 设置地理工作空间
    arcpy.env.workspace = dbpath

    cfg_xls = xlrd.open_workbook(cfgpath)
    cfg_sheet = cfg_xls.sheets()[0]  # 通过索引顺序获取

    nrows = cfg_sheet.nrows

    # 所有图层名
    lyrnames = cfg_sheet.col_values(0)[1:]




