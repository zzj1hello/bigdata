#!/usr/bin/env python
# @Time : 2021/7/12 16:01
# @desc : hdfs存储数据为orc格式
__coding__ = "utf-8"
__author__ = "zzjian"

from create_hive_table.cn.zzj.data2hive.fileformat.TableProperties import TableProperties


class OrcTableProperties(TableProperties):
    def getStoreFmtAndProperties(self, tableName):
        return "stored as orc\n"
