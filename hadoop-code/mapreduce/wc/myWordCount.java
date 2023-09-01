package com.zzj.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class myWordCount {

    // 要启动的JVM 进程  需要写在主函数中  Job类的文档里有运行示例代码
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true); // 加载conf
        // local
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 指定输入参数
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] other_args = parser.getRemainingArgs();
//        System.out.println(conf.get("mapreduce.job.reduces"));

        // MR的客户端类
        Job job = Job.getInstance(conf);
        job.setJarByClass(myWordCount.class); //业务逻辑代码的类 >>hdfs jar + 该类

        // Specify various job-specific parameters
        job.setJobName("myjob");
//        job.setNumReduceTasks(); 可以找到这个方法里头对应的配置属性为 mapreduce.job.reduces
        //  可以指定不同的数据源去给job添加Path 此处数据源为TextInputFormat；输入数据和输出数据
        Path infile = new Path(other_args[0]); // 目录可以写死的 以后可通过args指定
        TextInputFormat.addInputPath(job, infile);

        Path outfile = new Path(other_args[1]);
        // 通过对象方法删查 应该也可以FileSystem.get(conf) .exists() .delete() ;
        if (outfile.getFileSystem(conf).exists(outfile))
            outfile.getFileSystem(conf).delete(outfile, true);
        TextOutputFormat.setOutputPath(job, outfile);


        // 核心：补全业务逻辑代码类
        // 1 4 行：Hadoop 框架在运行时会使用 Java 的反射机制实例化对应的 Mapper 和 Reducer 对象
        // 中间两行M-》R 数据传递过程指定反射的对象类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
