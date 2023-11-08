package com.zzj.hadoop.mapreduce.Fof;

import com.zzj.hadoop.mapreduce.topn.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyFof {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFof.class);
        job.setJobName("Fof");
//        job.setNumReduceTasks(0); //检查map输出情况
        // 需要根据业务逻辑补充以下MR中的步骤

        // MapTask
        // input的输入格式化类
        TextInputFormat.addInputPath(job, new Path(other[0]));

        Path outPath = new Path(other[1]);
        if (outPath.getFileSystem(conf).exists(outPath))    outPath.getFileSystem(conf).delete(outPath, true);
        TextOutputFormat.setOutputPath(job, outPath);
        // map  无需添加OutputKeyClass  分区器和排序器
        job.setMapperClass(FofMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // combiner  直接用Reduce的类实现
//        job.setCombinerClass();

        //ReduceTask
        // shuffle 归并 不用做

        // reduce聚合数据
        job.setReducerClass(FofReducer.class);

        job.waitForCompletion(true);
    }

}
