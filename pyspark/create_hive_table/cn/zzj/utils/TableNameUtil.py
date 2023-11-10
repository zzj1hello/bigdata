#!/usr/bin/env python
# @desc : todo 用户将所有表名进行划分，构建全量列表和增量列表
__coding__ = "utf-8"
__author__ = "itcast"


def getODSTableNameList(fileNameList):
    """
    基于传递的所有表名，将增量表与全量表进行划分到不同的列表中
    :param fileNameList: 所有表名的列表
    :return: 增量与全量列表
    """
    full_list = []
    incr_list = []
    result_list = []
    isFull = True
    for line in fileNameList:
        if isFull:
            if "@".__eq__(line):
                isFull = False
                continue
            full_list.append(line)
        else:
            incr_list.append(line)
    result_list.append(full_list)
    result_list.append(incr_list)
    return result_list