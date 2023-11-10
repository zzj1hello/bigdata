#!/usr/bin/env python
# @desc : TODO 根据动态表，动态创建hive分区
__coding__ = "utf-8"
__author__ = "itcast"

from auto_create_hive_table.cn.itcast.datatohive import CreateMetaCommon
import logging


class CreateHiveTablePartition(object):

    def __init__(self, hiveConn):
        self.hiveConn = hiveConn

    def executeCPartition(self, dbName, hiveTName, dynamicDir, partitionDT):
        """
        用于实现给Hive表的数据手动申明分区
        :param dbName: 数据库名称
        :param hiveTName: 表名称
        :param dynamicDir: 全量或者增量
        :param partitionDT: 分区值
        :return: None
        """
        buffer = []
        cursor = None
        try:
            buffer.append("alter table " + dbName + ".")
            buffer.append(hiveTName)
            buffer.append(" add if not exists partition (dt='")
            buffer.append(partitionDT)
            buffer.append("') location '/data/dw/" + CreateMetaCommon.getDBFolderName(dbName) +
                          "/one_make/" + CreateMetaCommon.getDynamicDir(dbName, dynamicDir) + "/ciss4.")
            buffer.append(hiveTName)
            buffer.append("/")
            buffer.append(partitionDT)
            buffer.append("'")
            cursor = self.hiveConn.cursor()
            cursor.execute(''.join(buffer))
            # 输出日志
            logging.warning(f'执行创建hive\t{hiveTName}表的分区：{partitionDT},\t分区sql:\n{"".join(buffer)}')
        # 异常处理
        except Exception as e:
            print(e)
        # 释放游标
        finally:
            if cursor:
                cursor.close()
