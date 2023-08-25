package com.zzj.hadoop.mapreduce.wc;

import java.util.StringTokenizer;

public class str_itr {
    public static void main(String[] args) {
        StringTokenizer st = new StringTokenizer("this is a test\n this is b");

        while (st.hasMoreTokens()) {
            System.out.println(st.nextToken());
        }
    }
}
