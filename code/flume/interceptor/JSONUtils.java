package com.zzj.flume.interceptor;

public class JSONUtils {
    public static boolean isJSONValidate(String log){
        String jsonPattern = "^(\\{|\\[)(.*)(\\}|\\])$";
        boolean isJson = log.matches(jsonPattern);
        return isJson;
    }
}
