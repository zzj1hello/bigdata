#!/usr/bin/env python
# @desc : todo ODS&DWD建库、建表、装载数据主类
__coding__ = "utf-8"
__author__ = "zzj"

# 导入读Oracle表、建Hive表的包
from create_hive_table.cn.zzj.data2hive import CHiveTableFromOracleTable, CreateMetaCommon, CreateHiveTablePartition, LoadData2DWD
# 导入工具类：连接Oracle工具类、文件工具类、表名构建工具类
from create_hive_table.cn.zzj.utils import OracleHiveUtil, FileUtil, TableNameUtil
# 导入日志工具包
from create_hive_table.config import common

# 根据不同功能接口记录不同的日志
admin_logger = common.get_logger('zzj')


def recordLog(modelName):
    """
    记录普通级别日志
    :param modelName: 模块名称
    :return: 日志信息
    """
    msg = f'{modelName}'
    admin_logger.info(msg)
    return msg


def recordWarnLog(msg):
    """
    记录警告级别日志
    :param msg: 日志信息
    :return: 日志信息  warning级别日志（debug info warning error 级别递增 显示的信息越少）
    """
    admin_logger.warning(msg)
    return msg


if __name__ == '__main__':

    # =================================todo: 1-初始化Oracle、Hive连接，读取表的名称=========================#
    # 输出信息
    recordLog('ODS&DWD Building AND Load Data')
    partitionVal = '20210101' # 定义分区变量
    # oracleConn = OracleHiveUtil.getOracleConn()
    hiveConn = OracleHiveUtil.getSparkHiveConn()
    tableList = FileUtil.readFileContent("D:\\PythonProject\\OneMake_Spark\\dw\\ods\\meta_data\\tablenames.txt")
    tableNameList = TableNameUtil.getODSTableNameList(tableList)
    # ------------------测试：输出获取到的连接以及所有表名
    # print(oracleConn)
    # print(hiveConn)
    # for tbnames in tableNameList:
    #     print("---------------------")
    #     for tbname in tbnames:
    #         print(tbname)

    # =================================todo: 2-ODS层建库=============================================#
    cHiveTableFromOracleTable = CHiveTableFromOracleTable(oracleConn, hiveConn)
    # 打印日志
    recordLog('ODS层创建数据库')
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.ODS_NAME)

    # =================================todo: 3-ODS层建表=============================================#
    # 打印日志
    recordLog('ODS层创建全量表...')
    fullTableList = tableNameList[0]
    for tblName in fullTableList:
        cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.FULL_IMP)
    # 打印日志
    recordLog('ODS层创建增量表...')
    incrTableList = tableNameList[1]
    for tblName in incrTableList:
        # Hive中创建这张增量表：数据库名称、表名、表的类型
        cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.INCR_IMP)

    # =================================todo: 4-ODS层申明分区=============================================#
    recordLog('创建ods层全量表分区...')
    createHiveTablePartition = CreateHiveTablePartition(hiveConn)
    # 全量表执行44次创建分区操作
    for tblName in fullTableList:
        createHiveTablePartition.executeCPartition(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.FULL_IMP, partitionVal)

    recordLog('创建ods层增量表分区...')
    # 增量表执行57次创建分区操作
    for tblName in incrTableList:
        createHiveTablePartition.executeCPartition(CreateMetaCommon.ODS_NAME, tblName, CreateMetaCommon.INCR_IMP, partitionVal)

    # =================================todo: 5-DWD层建库建表=============================================#
    # 5.1 建库记录日志
    recordLog('DWD层创建数据库')
    # 创建DWD层数据库
    cHiveTableFromOracleTable.executeCreateDbHQL(CreateMetaCommon.DWD_NAME)

    # 5.2 建表记录日志
    recordLog('DWD层创建表...')
    allTableName = [i for j in tableNameList for i in j]
    for tblName in allTableName:
        cHiveTableFromOracleTable.executeCreateTableHQL(CreateMetaCommon.DWD_NAME, tblName, None)

    # =================================todo: 6-DWD层数据抽取=============================================#
    # 记录日志
    recordWarnLog('DWD层加载数据，此操作将启动Spark JOB执行，请稍后...')
    for tblName in allTableName:
        recordLog(f'加载dwd层数据到{tblName}表...')
        try:
            LoadData2DWD.loadTable(oracleConn, hiveConn, tblName, partitionVal)
        except Exception as error:
            print(error)
        recordLog('完成!!!')

# =================================todo: 7-程序结束，释放资源=============================================#
oracleConn.close()
hiveConn.close()
