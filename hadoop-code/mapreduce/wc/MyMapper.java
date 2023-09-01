package com.zzj.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    //指定输入的kv 输出的kv 他们的类型  IntWritable 类是 Writable 接口的实现类，用于表示可序列化的整数值。
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    // TextInputFormat输入格式化类：key是字符对于文本文件中的偏移量 value是文本每行的字符串
    // 输出的是上面定义的word和one
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 字符串处理类 能接收不同的正则方式进行split 切割出每个单词
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            // write也会对word one序列化 而不是引用对象 否则会再下次对引用对象word修改了 影响数据
            // word写在外面 能减少GC
            context.write(word, one);
        }
    }
}
