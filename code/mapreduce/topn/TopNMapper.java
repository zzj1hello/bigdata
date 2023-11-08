package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;


public class TopNMapper extends Mapper<LongWritable, Text, TopNKey, IntWritable> {

    // 从input中生成可序列化的记录

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("y-M-d HH:mm:ss");
    TopNKey mkey = new TopNKey();
    IntWritable mval = new IntWritable();

    public HashMap<String, String> dict = new HashMap<String, String>();
    @Override
    protected void setup(Mapper<LongWritable, Text, TopNKey, IntWritable>.Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        Path path = new Path(files[0].getPath());
        // 读取该本地Container的目录文件到Buffer中 返回的是首行字符指针，readline移动指针当下一行
        BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
        String line = reader.readLine();
        while(line != null){
            String[] line_split = line.split(",");
            dict.put(line_split[0], line_split[1]);
            line = reader.readLine();
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 2022-8-1 22:22:22   1   30 --> 按\t切割字符串存入key类中
        String[] strs = value.toString().split(",");

        // 解析字符串为 LocalDateTime 对象
        LocalDateTime date = LocalDateTime.parse(strs[0], formatter);
        mkey.setYear(date.getYear());
        mkey.setMonth(date.getMonthValue());
        mkey.setDay(date.getDayOfMonth());
        int temp = Integer.parseInt(strs[2]);
        mkey.setTemp(temp);
        mval.set(temp);
        mkey.setCity(dict.get(strs[1])); // 映射出城市名称
        context.write(mkey, mval);
    }
}
