package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TopNSortComparator extends WritableComparator {

    public TopNSortComparator(){
        super(TopNKey.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //  Returns: 返-1 0 1
        // 使用TopNKey类型的进行比较 使用.compare()方法返回结果

        TopNKey k1 = (TopNKey) a;
        TopNKey k2 = (TopNKey) b;

        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if ( c1==0) {
            int c2 = Integer.compare(k1.getMonth(), k2.getMonth());
            if (c2==0)  return -Integer.compare(k1.getTemp(), k2.getTemp());
            return c2;
        }
        return c1;
    }
}