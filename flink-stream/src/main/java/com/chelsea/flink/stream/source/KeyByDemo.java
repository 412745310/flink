package com.chelsea.flink.stream.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * keyby操作
 * 
 * @author shevchenko
 *
 */
public class KeyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("47.107.247.223", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;
            
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = in.split(" ");
                for(String w : words) {
                    out.collect(new Tuple2<>(w, 1));
                }
            }
            
        });
        // 先对元组的第一个字段分组，再对分组后的元组第二个字段求和
        flatMap.keyBy(0).sum(1).print();
        env.execute();
    }

}
