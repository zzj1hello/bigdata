package com.zzj.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.htrace.fasterxml.jackson.databind.util.JSONPObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;


public class TimeStampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders(); // 请求头存放元数据
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject json = JSONObject.parseObject(log);
        String ts = json.getString("time");
        headers.put("timestamp", ts);
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
