package me.pengbo.flume.interceptors;


import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.source.kafka.KafkaSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 针对kafka source
 * 将日志中的日期转换成timestamp加入到header中
 * 将topic信息设置到指定名称的header中(默认设置 kafka source中，setTopicHeader=true,topicHeader=topic)
 * Created by pengbo on 18-3-26.
 */
public class ExtractLogTimestampAndKafkaTopicToHeaderInterceptor implements Interceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractLogTimestampToHeaderInterceptor.class);

    private static Clock clock = new SystemClock();

    private final Pattern regex;

    private DateTimeFormatter formatter;

    private String hadoopDir;

    private String topicHeader;

    public ExtractLogTimestampAndKafkaTopicToHeaderInterceptor(String extractRegex,
            String dateTimePattern, String hadoopDir, String topicHeader){
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

        this.hadoopDir = hadoopDir;

        this.topicHeader = topicHeader;
    }

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        for(Map.Entry<String, String> entry :headers.entrySet()){
            System.out.println(entry.getKey()+"-"+entry.getValue());
        }
        if(hadoopDir != null && !"".equals(hadoopDir.trim())){
            String topic = headers.get(this.topicHeader);
            String partitioner = headers.get(KafkaSourceConstants.PARTITION_HEADER);
            headers.put(hadoopDir, topic + partitioner);
        }
        Matcher matcher = regex.matcher(new String(event.getBody(), Charsets.UTF_8));
        if(matcher.find()){
            String dateTimeString = matcher.group(1);
            try{
                LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, formatter);
                long timestamp = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                headers.put("timestamp", String.valueOf(timestamp));
            }catch (Exception e){
                LOGGER.warn("日期转换错误,使用系统当前时间",e);
                headers.put("timestamp", String.valueOf(clock.currentTimeMillis()));
            }
        }else{
            headers.put("timestamp", String.valueOf(clock.currentTimeMillis()));
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList intercepted = Lists.newArrayListWithCapacity(list.size());
        for(Event event : list){
            Event interceptedEvent = intercept(event);
            if(interceptedEvent != null){
                LOGGER.debug("extracted timestamp:{}", interceptedEvent.getHeaders().get("timestamp"));
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }
    public static class ExtractLogTimestampAndKafkaTopicToHeaderInterceptorBuilder implements Builder{

        private static final String formatter = "formatter";//日期格式化格式
        private static final String extract = "extract";//抽取日志部分正则
        private static final String hadoopDir = "hadoopDir";//自定义hadoop文件加名称

        private static final String setTopicHeader = "setTopicHeader";
        private static final String topicHeader = "topicHeader";

        private Context context;

        @Override
        public Interceptor build() {
            String dateTimeFormatter = context.getString(formatter);
            String extractRegex = context.getString(extract);
            String hadoopDirStr = context.getString(hadoopDir);
            //是否设置主题
            boolean setTopicHeaderb = context.getBoolean(setTopicHeader, true);
            //主题名称
            String topicHeaderName = "";
            if(setTopicHeaderb){
                topicHeaderName = context.getString(topicHeader, KafkaSourceConstants.DEFAULT_TOPIC_HEADER);
            }

            return new ExtractLogTimestampAndKafkaTopicToHeaderInterceptor(extractRegex,
                    dateTimeFormatter, hadoopDirStr, topicHeaderName);
        }

        @Override
        public void configure(Context context) {
            this.context = context;
        }
    }
}
