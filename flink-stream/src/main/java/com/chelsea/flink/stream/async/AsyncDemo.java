package com.chelsea.flink.stream.async;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

/**
 * 异步IO示例
 *
 */
public class AsyncDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<Integer> collectionDs = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5));
        SingleOutputStreamOperator<String> resDs = AsyncDataStream.unorderedWait(collectionDs, new MyAsyncFunction(), 10000, TimeUnit.MILLISECONDS);
        resDs.print();
        env.execute();
    }
    
    /**
     * 自定义异步IO操作类
     * 
     */
    static class MyAsyncFunction extends RichAsyncFunction<Integer, String> {

        private static final long serialVersionUID = 1L;
        
        /**
         * 初始化方法，可以做一些外部系统的初始化连接
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化");
        }
        
        /**
         * 关闭连接
         */
        @Override
        public void close() throws Exception {
            System.out.println("关闭");
        }
        
        /**
         * 处理异步请求超时方法，默认超时会报异常，甚至导致程序退出
         */
        @Override
        public void timeout(Integer input, ResultFuture<String> resultFuture) throws Exception {
            System.out.println("超时");
        }

        /**
         * 执行的主要方法，例如查询数据库数据，redis数据等
         * flink所谓的异步IO，并不是只要实现了asyncInvoke方法就是异步了，这个方法并不是异步的，而是要依靠这个方法里面所写的查询是异步的才可以
         */
        @Override
        public void asyncInvoke(Integer in, ResultFuture<String> resultFuture) throws Exception {
            System.out.println("异步执行");
            CompletableFuture.supplyAsync(new Supplier<Integer>() {

                @Override
                public Integer get() {
                    try {
                        // 模拟查询时所花费的时间
                        Thread.sleep(5000);
                        return in;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }).thenAccept((Integer dbResult) -> {
                resultFuture.complete(Collections.singleton(String.valueOf(dbResult)));
            });
        }
        
    }

}
