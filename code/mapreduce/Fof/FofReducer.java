package com.zzj.hadoop.mapreduce.Fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rval = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 马老师 一名老师 0
        // 马老师 一名老师 1
        // 一组间接关系的value值有1 则退出迭代，否则返回key和val和（表示这对直接关系中间接关系出现的数量）

        int flg = 0;
        int sum = 0;
        for (IntWritable val: values){   // 注意迭代器的取值
            if (val.get() == 0){
                flg = 1;
                break;
            }
            sum += val.get();
        }
        if (flg == 0) {
            rkey.set(key);
            rval.set(sum);
            context.write(rkey, rval);
        }
    }
}
