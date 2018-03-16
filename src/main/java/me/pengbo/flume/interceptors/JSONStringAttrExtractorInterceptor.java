package me.pengbo.flume.interceptors;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Log4j2 日志文件每一行为json数据，将指定属性的值加入到event header中
 * config [agent].sources.[source].interceptors.[intercptor].jsonAttr=marker.name
 * Created by pengbo on 18-3-14.
 */
public class JSONStringAttrExtractorInterceptor implements Interceptor{

    private static final Logger LOGGER = LoggerFactory.getLogger(JSONStringAttrExtractorInterceptor.class);

    private String configure;

    private String[] values;

    public JSONStringAttrExtractorInterceptor(String configure){
        this.configure = configure == null ? null : configure.trim();
        if(configure != null && configure != ""){
            this.values = configure.split("\\.");
        }
    }

    private static Gson gson = new Gson();

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        if(values == null || values.length == 0)return event;

        String body = new String(event.getBody(), Charset.forName("UTF-8"));
        Map map = gson.fromJson(body, Map.class);
        for(int i = 0; i < values.length; i ++){
            Object obj = map.get(values[i]);
            if(obj == null){
                return null;
            }else{
                //是map
                if( obj instanceof Map){
                    //不是最后一层
                    if(i != values.length -1){
                        map = (Map)obj;
                        continue;
                    }else{
                        //是最后一层属性
                        return null;
                    }
                }else{
                    //非map,是最后一个属性
                    if(i == values.length - 1){
                        event.getHeaders().put(configure, obj.toString());
                        return event;
                    }else{
                        return null;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        ArrayList intercepted = Lists.newArrayListWithCapacity(list.size());
        for(Event event : list){
            Event interceptedEvent = intercept(event);
            if(interceptedEvent != null){
                LOGGER.debug("marker.name:{}", interceptedEvent.getHeaders().get("marker.name"));
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }

    public static class JSONStringAttrExtractorInterceptorBuilder implements Builder{

        private static final String HEADER_KEY = "jsonAttr";

        private Context context;

        @Override
        public Interceptor build() {
            String configure = context.getString(HEADER_KEY);
            if(configure == null){
                return new me.pengbo.flume.interceptors.JSONStringAttrExtractorInterceptor(null);
            }else{
                configure = configure.trim();
                return configure == ""
                        ? new me.pengbo.flume.interceptors.JSONStringAttrExtractorInterceptor(null)
                        : new me.pengbo.flume.interceptors.JSONStringAttrExtractorInterceptor(configure);
            }
        }

        @Override
        public void configure(Context context) {
            this.context = context;
        }
    }
}
