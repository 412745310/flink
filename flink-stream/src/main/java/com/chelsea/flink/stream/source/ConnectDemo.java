package com.chelsea.flink.stream.source;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 流连接demo
 * 
 * @author shevchenko
 *
 */
public class ConnectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> numDs = env.addSource(new MyNumberSource());
        DataStreamSource<String> strDs = env.addSource(new MyStrSource());
        ConnectedStreams<Long, String> connect = numDs.connect(strDs);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<Long, String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String map1(Long in) throws Exception {
                return String.valueOf(in);
            }

            @Override
            public String map2(String in) throws Exception {
                return in;
            }});
        map.print();
        env.execute();
    }
    
    static class MyNumberSource implements SourceFunction<Long> {
        private static final long serialVersionUID = 1L;
        private boolean isRunning = true;
        private Long count = 0l;

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void run(SourceContext<Long> out) throws Exception {
            while(isRunning) {
                count++;
                out.collect(count);
                Thread.sleep(1000);
            }
        }
    }
    
    static class MyStrSource implements SourceFunction<String> {
        private static final long serialVersionUID = 1L;
        private boolean isRunning = true;
        private Long count = 0l;

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void run(SourceContext<String> out) throws Exception {
            while(isRunning) {
                count++;
                out.collect("str" + count);
                Thread.sleep(1000);
            }
        }
    }

}
