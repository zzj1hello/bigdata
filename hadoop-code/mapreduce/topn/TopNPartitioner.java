package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNPartitioner extends Partitioner<TopNKey, IntWritable> {
    @Override
    public int getPartition(TopNKey key, IntWritable value, int numPartitions) {
        // 直接根据年份作为hashcode   固有可能存在数据倾斜的问题 因为一些年份的数据会特别多 早些的年份数据比较少
        return key.getYear() % numPartitions;
    }
}
