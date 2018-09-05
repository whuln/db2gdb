# -*- coding: utf-8 -*-
import arcpy
import os

arcpy.env.workspace = 'E:/ahdata/gdbserver/slpc.gdb'

fcs = arcpy.ListFeatureClasses()

for fc in fcs:
    arcpy.AddField_management(os.path.join(arcpy.env.workspace, fc), 'FLAG', "TEXT", field_is_nullable="NULLABLE")
