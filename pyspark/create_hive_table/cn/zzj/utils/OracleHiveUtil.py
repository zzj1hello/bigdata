#!/usr/bin/env python
# @desc : todo 实现构建Oracle、Hive、SparkSQL的连接
__coding__ = "utf-8"
__author__ = "itcast"

# 导包
from create_hive_table.cn.zzj.utils import ConfigLoader         # 导入配置文件解析包
# import cx_Oracle                                                        # 导入Python连接Oracle依赖库包
# from pyhive import hive                                                 # 导入Python连接Hive依赖包
import os                                                               # 导入系统包

# 配置Oracle的客户端驱动文件路径
LOCATION = r"D:\\instantclient_12_2"
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]


def getOracleConn():
    """
    用户获取Oracle的连接对象：cx_Oracle.connect(host='', port='', username='', password='', param='')
    :return:
    """
    oracleConn = None   #构建Oracle连接对象
    try:
        ORACLE_HOST = ConfigLoader.getOracleConfig('oracleHost')
        ORACLE_PORT = ConfigLoader.getOracleConfig('oraclePort')
        ORACLE_SID = ConfigLoader.getOracleConfig('oracleSID')
        ORACLE_USER = ConfigLoader.getOracleConfig('oracleUName')
        ORACLE_PASSWORD = ConfigLoader.getOracleConfig('oraclePassWord')
        dsn = cx_Oracle.makedsn(ORACLE_HOST, ORACLE_PORT, ORACLE_SID)
        oracleConn = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, dsn)
    # 异常处理
    except cx_Oracle.Error as error:
        print(error)
    return oracleConn


def getSparkHiveConn():
    """
    用户获取SparkSQL的连接对象
    :return:
    """
    # 构建SparkSQL的连接对象
    sparkHiveConn = None
    try:
        SPARK_HIVE_HOST = ConfigLoader.getSparkConnHiveConfig('sparkHiveHost')
        SPARK_HIVE_PORT = ConfigLoader.getSparkConnHiveConfig('sparkHivePort')
        SPARK_HIVE_UNAME = ConfigLoader.getSparkConnHiveConfig('sparkHiveUName')
        SPARK_HIVE_PASSWORD = ConfigLoader.getSparkConnHiveConfig('sparkHivePassWord')
        sparkHiveConn = hive.Connection(host=SPARK_HIVE_HOST, port=SPARK_HIVE_PORT, username=SPARK_HIVE_UNAME, auth='CUSTOM', password=SPARK_HIVE_PASSWORD)
    # 异常处理
    except Exception as error:
        print(error)
    return sparkHiveConn


def getHiveConn():
    """
    用户获取HiveServer2的连接对象
    :return:
    """
    # 构建Hive的连接对象
    hiveConn = None
    try:
        HIVE_HOST= ConfigLoader.getHiveConfig("hiveHost")
        HIVE_PORT= ConfigLoader.getHiveConfig("hivePort")
        HIVE_USER= ConfigLoader.getHiveConfig("hiveUName")
        HIVE_PASSWORD= ConfigLoader.getHiveConfig("hivePassWord")
        hiveConn = hive.Connection(host=HIVE_HOST,port=HIVE_PORT,username=HIVE_USER,auth='CUSTOM',password=HIVE_PASSWORD)
    # 异常处理
    except Exception as error:
        print(error)
    return hiveConn

