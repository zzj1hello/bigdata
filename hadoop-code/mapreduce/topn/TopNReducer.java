package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TopNReducer extends Reducer<TopNKey, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rvalue = new IntWritable();
    @Override
    protected void reduce(TopNKey key, Iterable<IntWritable> values, Reducer<TopNKey, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 将每条输入key中年月日中的日记录 判断不可重复 判断是否累计两条
        Iterator<IntWritable> itr = values.iterator();
        int cnt = 0;
        int flag = -1;
        while (itr.hasNext()){ // 每个年月分组的迭代
            if (cnt == 2)   break;

            itr.next(); // 虽然迭代器里的值可以从key中取， 但还是要执行.next() 否则指针不动 key value也不会变化
            int day = key.getDay();
            if (flag == day) continue;

            rkey.set(key.getYear()+"-"+key.getMonth()+"-"+day+"@"+key.getCity());
            rvalue.set(key.getTemp());
            flag = day;
            cnt += 1;
            context.write(rkey, rvalue);
        }
    }
}
