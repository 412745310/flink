package com.chelsea.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用ValueState存储中间结果并求出最大值
 * 
 * @author shevchenko
 *
 */
public class ValueStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("47.107.247.223", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> map(String in) throws Exception {
                String[] words = in.split(",");
                return new Tuple2<>(words[0], Integer.valueOf(words[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyBy = map.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxDs = keyBy.map(new RichMapFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {

            private static final long serialVersionUID = 1L;
            
            private ValueState<Integer> valueState = null;
            
            /**
             * 每个线程只执行一次，创建ValueState对象
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("maxValue", Integer.class, 0);
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
                // 获取ValueState保存的中间结果
                Integer maxValue = valueState.value();
                if (in.f1 > maxValue) {
                    // 更新ValueState的中间结果
                    valueState.update(in.f1);
                }
                return new Tuple2<>(in.f0, valueState.value());
            }
        });
        maxDs.print();
        env.execute();
    }

}
