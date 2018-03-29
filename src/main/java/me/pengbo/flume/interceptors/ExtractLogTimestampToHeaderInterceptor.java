package me.pengbo.flume.interceptors;


import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将日志中的日期转换成timestamp加入到header中
 * Created by pengbo on 18-3-26.
 */
public class ExtractLogTimestampToHeaderInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractLogTimestampToHeaderInterceptor.class);

    private static final String TIMESTAMP_KEY = "timestamp";

    private static Clock clock = new SystemClock();

    private final Pattern regex;

    private DateTimeFormatter formatter;

    public ExtractLogTimestampToHeaderInterceptor(String extractRegex, String dateTimePattern){
        //日期格式化
        if(dateTimePattern != null && !"".equals(dateTimePattern.trim())){
           formatter = DateTimeFormatter.ofPattern(dateTimePattern);
        }else{
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        }
        //抽取正则
        if(extractRegex != null && !"".equals(extractRegex.trim())){

            regex = Pattern.compile(extractRegex);
        }else{
            extractRegex = "(yyyy-MM-dd HH:mm:ss.SSS)";
            regex = Pattern.compile(extractRegex);
        }
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        Matcher matcher = regex.matcher(new String(event.getBody(), Charsets.UTF_8));
        if(matcher.find()){
            String dateTimeString = matcher.group(1);
            try{
                LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
                long timestamp = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                headers.put(TIMESTAMP_KEY, String.valueOf(timestamp));
            }catch (Exception e){
                LOGGER.warn("日期转换错误,使用系统当前时间",e);
                if(headers.get(TIMESTAMP_KEY) == null){
                    headers.put(TIMESTAMP_KEY, String.valueOf(clock.currentTimeMillis()));
                }
            }
        }else{
            if(headers.get(TIMESTAMP_KEY) == null){
                headers.put(TIMESTAMP_KEY, String.valueOf(clock.currentTimeMillis()));
            }
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList intercepted = Lists.newArrayListWithCapacity(list.size());
        for(Event event : list){
            Event interceptedEvent = intercept(event);
            if(interceptedEvent != null){
                LOGGER.debug("extracted timestamp:{}", interceptedEvent.getHeaders().get(TIMESTAMP_KEY));
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }
    public static class ExtractLogTimestampToHeaderInterceptorBuilder implements Builder{

        private static final String formatter = "formatter";//日期格式化格式
        private static final String extract = "extract";//抽取日志部分正则

        private Context context;

        @Override
        public Interceptor build() {
            String dateTimeFormatter = context.getString(formatter);
            String extractRegex = context.getString(extract);
            return new ExtractLogTimestampToHeaderInterceptor(extractRegex, dateTimeFormatter);
        }

        @Override
        public void configure(Context context) {
            this.context = context;
        }
    }

    public static void main(String[] args) {
        String a= "[2011-12-07 22:22:22]xxxxxxxxxxx[2311-12-07 22:22:22]xsflasjg;l;;;[2411-12-07 22:22:22]asdfsf";
        Pattern p = Pattern.compile("\\[(\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d)\\]");
        Matcher m = p.matcher(a);
        while(m.find()){
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localDateTime = LocalDateTime.parse(m.group(1), dateTimeFormatter);
            System.out.println(localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli());
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = df.parse("2011-12-07 22:22:22");
            System.out.println(date.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
