#!/usr/bin/env python
# @desc : todo 实现读取表名文件
__coding__ = "utf-8"
__author__ = "zzjian"


def readFileContent(fileName):
    """
    加载表名所在的文件
    :param fileName:存有表名的文件路径
    :return:存有所有表名的列表集合
    """
    tableNameList = []
    fr = open(fileName)
    for line in fr.readlines():
        curLine = line.rstrip('\n')
        tableNameList.append(curLine)
    return tableNameList
