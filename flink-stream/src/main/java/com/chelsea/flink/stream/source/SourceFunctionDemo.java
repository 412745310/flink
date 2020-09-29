package com.chelsea.flink.stream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 自定义非并行数据源
 * 
 * @author shevchenko
 *
 */
public class SourceFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> addSource = env.addSource(new MyNoParalleSourceFunction());
        addSource.print();
        env.execute();
    }
    
    static class MyNoParalleSourceFunction implements SourceFunction<Long> {

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

}
