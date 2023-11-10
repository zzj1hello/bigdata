#!/usr/bin/env python
# @desc : todo 功能一：创建ODS层数据库。功能二：从oracle中获得表结构，在hive中创建对应表。
__coding__ = "utf-8"
__author__ = "zzj"

# 导包
from pyhive import hive                                                         # 导入Hive操作包
from create_hive_table.cn.zzj.data2hive import CreateMetaCommon        # 导入常量数据包
from create_hive_table.cn.zzj.utils import OracleMetaUtil               # 导入Oracle表信息的工具类
import logging                                                                  # 导入日志记录包


class CHiveTableFromOracleTable:

    # 构建当前类的对象时,初始化Oracle和Hive的连接
    def __init__(self, oracleConn, hiveConn):
        self.oracleConn = oracleConn
        self.hiveConn = hiveConn

    # 创建数据库方法
    def executeCreateDbHQL(self, dbName):
        """
        根据传递的数据库名称，在Hive中创建数据库
        :param dbName: 数据库名称
        :return: None
        """
        createDbHQL = 'create database if not exists ' + dbName
        cursor = self.hiveConn.cursor()
        try:
            cursor.execute(createDbHQL)
        # 异常处理
        except hive.Error as error:
            print(error)
        # 执行结束，最后释放游标
        finally:
            if cursor:
                cursor.close()

    # 执行Hive建表
    def executeCreateTableHQL(self, dbName, tableName, dynamicDir):
        """
        用于根据传递的数据库名称、表名在Hive中创建对应的表，self为当前类的实例对象
        :param dbName: 数据库名称【ODS、DWD】
        :param tableName: 表名
        :param dynamicDir: 全量或者增量【full_imp、incr_imp】
        :return: None
        """
        buffer = []
        cursor = None
        try:
            tableMeta = OracleMetaUtil.getTableMeta(self.oracleConn, tableName.upper())
            buffer.append("create external table if not exists " + dbName + ".")
            buffer.append(tableName.lower())
            buffer = getODSStringBuffer(buffer, dbName, tableMeta)
            if tableMeta.tableComment:
                buffer.append(" comment '" + tableMeta.tableComment + "' \n")
            buffer.append(' partitioned by (dt string) ')
            buffer.append(CreateMetaCommon.getTableProperties(dbName, tableName))
            dbFolderName = CreateMetaCommon.getDBFolderName(dbName)
            userName = CreateMetaCommon.getUserNameByDBName(dbName)
            buffer.append(" location '/data/dw/" + dbFolderName + "/one_make/" + CreateMetaCommon.getDynamicDir(dbName,dynamicDir) + "/" + userName + tableName + "'")
            cursor = self.hiveConn.cursor()
            cursor.execute(''.join(buffer))
            logging.warning(f'oracle表转换{dbFolderName}后的Hive DDL语句为:\n{"".join(buffer)}')
        # 异常处理
        except Exception as exception:
            print(exception)
        # 释放游标
        finally:
            if cursor:
                cursor.close()


# 根据数据库得到部分建表语句，ODS 与 DWD差别部分处理
def getODSStringBuffer(buffer, dbName, tableMeta):
    """
    用于实现将Oracle的列的信息，解析为Hive的列的信息，实现类型转换等
    :param buffer: 当前拼接的建表语句
    :param dbName: 当前数据库名称
    :param tableMeta: 当前表的信息
    :return: None
    """
    simpleName = CreateMetaCommon.getDBFolderName(dbName)
    if not 'ods'.__eq__(simpleName):
        buffer.append('(\n\t')
        for cmeta in tableMeta.columnMetaList:
            # 将Oracle的类型转换为Hive的类型：timestamp => long , number => bigint | dicimal ,other => String
            hiveDataType = convertDataType(cmeta.dataType, cmeta.dataScale, cmeta.dataScope)
            # 添加列的名称
            buffer.append(cmeta.columnName)
            # 添加空格
            buffer.append(' ')
            # 添加列的类型
            buffer.append(hiveDataType)
            # 如果该列有注释，则添加注释
            if cmeta.columnComment:
                buffer.append(" comment '")
                buffer.append(cmeta.columnComment)
                buffer.append("'")
            # 在列的定义后加上逗号
            buffer.append(',\n\t')
        # 循环结束后，把最后一列的那个逗号删除
        buffer.pop(-1)
        # 添加最后的括号
        buffer.append('\n)')
    # 返回列的定义
    return buffer


def convertDataType(oracleDType: str, dataScale, dataScope):
    """
    将Oracle中列的类型转换为Hive中的数据类型
    :param oracleDType: 列的类型
    :param dataScale: 列的长度
    :param dataScope: 列的精度
    :return:
    """
    # 字段名称和字段类型不为空
    if oracleDType:
        # 如果Oracle中为timestamp，返回long类型,注意:long类型Hive不支持，SparkSQL支持
        if oracleDType.startswith('TIMESTAMP'):
            return 'long'
        # 如果Oracle中为数值类型
        elif equalsIgnoreCase('NUMBER', oracleDType):
            # 如果长度为None或者长度小于1
            if dataScale is None or dataScale < 1:
                # 整数类型，返回bigint
                return 'bigint'
            # 为数值，但是有小数点
            else:
                # 返回dicimal类型
                return f'decimal({dataScope}, {dataScale})'
        # 其他类型全部返回String类型
        else:
            return 'string'
    else:
        print('未获取到字段对应类型')


def equalsIgnoreCase(a, b):
    """
    比较两个字符串，并不区分大小写
    """
    if isinstance(a, str):
        if isinstance(b, str):
            return len(a) == len(b) and a.upper() == b.upper()
    return False
