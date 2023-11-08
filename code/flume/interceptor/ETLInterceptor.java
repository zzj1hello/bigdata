package com.zzj.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 重载拦截方法
        byte[] body = event.getBody();  // 请求体存放数据，取到二进制数据
        String log = new String(body, StandardCharsets.UTF_8); //转化为文本
        if (JSONUtils.isJSONValidate(log)) // 判断是否为json格式数据
            return event;
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        // 不为json格式的event 封装成列表返回
        Iterator<Event> itr = list.iterator();
        while (itr.hasNext()){
            Event next = itr.next();
            if (intercept(next) == null)
                itr.remove();
        }
        return null;
    }
    // 放到静态类中给Flume组件执行
    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
    @Override
    public void close() {

    }
}
