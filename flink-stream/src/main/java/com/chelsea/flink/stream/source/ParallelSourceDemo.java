package com.chelsea.flink.stream.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 自定义并行数据源
 * 
 * @author shevchenko
 *
 */
public class ParallelSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为3，意味着有3个task线程同时在执行source源的run方法
        DataStreamSource<Long> addSource = env.addSource(new MyParalleSourceFunction()).setParallelism(3);
        addSource.print();
        env.execute();
    }
    
    static class MyParalleSourceFunction extends RichParallelSourceFunction<Long> {

        private static final long serialVersionUID = 1L;
        private boolean isRunning = true;
        private Long count = 0l;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化方法，每个task线程只执行一次");
        }
        
        @Override
        public void close() throws Exception {
            System.out.println("资源回收方法，每个task线程只执行一次");
        }

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
