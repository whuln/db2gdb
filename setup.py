# -*- coding: utf-8 -*-
from distutils.core import setup
import py2exe


setup(console=['db2gdb.py'],options = { "py2exe":{"includes":["sip"],"packages": ["sqlalchemy.sql","sqlalchemy.dialects.oracle"], "excludes": ["arcpy"],"dll_excludes":["MSVCP90.dll"]}})