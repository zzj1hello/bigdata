package com.zzj.hadoop.mapreduce.Fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text mkey = new Text();
    IntWritable mval = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // value: 马老师 一名老师 刚老师 周老师
        String[] strs = value.toString().split(" ");
        for(int i=1; i<strs.length; i++){
            // 直接关系 01 02 03
            mkey.set(getFof(strs[0], strs[i]));
            mval.set(0);  // 赋值间接关系数为0
            context.write(mkey, mval);
            // 间接关系 12 13 23
            for(int j=i+1; j< strs.length; j++){
                mkey.set(getFof(strs[i], strs[j]));
                mval.set(1); // 赋值间接关系数为1
                context.write(mkey, mval);
            }
        }
    }
    public static String getFof(String s1, String s2){
        if (s1.compareTo(s2) > 0){
            return  s1 + " " + s2;
        }
        return s2 + " " + s1;
    }
}
