package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;


public class MyTopN {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTopN.class);
        job.setJobName("TopN");

        // 发送map端join的文件
        job.setCacheFiles(new URI[]{new Path("input/dict.txt").toUri()});

        // 需要根据业务逻辑补充以下MR中的步骤

        // MapTask
        // input的输入格式化类
        TextInputFormat.addInputPath(job, new Path(other[0]));

        Path outPath = new Path(other[1]);
        if (outPath.getFileSystem(conf).exists(outPath))    outPath.getFileSystem(conf).delete(outPath, true);
        TextOutputFormat.setOutputPath(job, outPath);

        // key的类型实现（用于按照key的排序比较器）和map
        job.setMapperClass(TopNMapper.class);
        job.setMapOutputKeyClass(TopNKey.class);
        job.setMapOutputValueClass(IntWritable.class); // 内置的类

        // partitioner  按年分区
        job.setPartitionerClass(TopNPartitioner.class);

        // sortComparator  按年月 温度排序 (默认用的快排算法 不变)
        job.setSortComparatorClass(TopNSortComparator.class);

        // combiner  直接用Reduce的类实现
//        job.setCombinerClass();

        //ReduceTask
        // shuffle 归并 不用做

        // groupingComparator
        job.setGroupingComparatorClass(TopNGroupingComparator.class);
        // reduce聚合数据
        job.setReducerClass(TopNReducer.class);

        job.waitForCompletion(true);
    }
}
