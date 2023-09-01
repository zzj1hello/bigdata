package com.zzj.hadoop.mapreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 实现接口：定义序列化反序列化  比较器
public class TopNKey implements WritableComparable<TopNKey> {

    private int year;
    private int month;
    private int day;
    private int temp;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    private String city;
    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public int compareTo(TopNKey other) {
        // Integer.compare的Returns:the value 0 if x == y; a value less than 0 if x < y; and a value greater than 0 if x > y
        int c1 = Integer.compare(this.year, other.getYear()); // 先判断年
        if (c1==0){
            int c2 = Integer.compare(this.month, other.getMonth()); // 继续判断月
            if (c2 == 0){
                return  Integer.compare(this.day, other.getDay());
            }
            return c2;
        }
        return c1;
    }


    public void write(DataOutput out) throws IOException {
        // 每个int变量占4字节 组成16字节 存到字节数组中
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(temp);
        out.writeUTF(city);
    }

    public void readFields(DataInput in) throws IOException {
        // 接收端拿到字节数组 从数组中赋值
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.temp = in.readInt();
        this.city = in.readUTF();
    }

}
