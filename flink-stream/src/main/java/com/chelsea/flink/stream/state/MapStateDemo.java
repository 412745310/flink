package com.chelsea.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用MapState存储中间结果并求出最大值
 * 
 * @author shevchenko
 *
 */
public class MapStateDemo {

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
            
            private MapState<String, Integer> mapState = null;
            
            /**
             * 每个线程只执行一次，创建ValueState对象
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("maxValue", TypeInformation.of(String.class), TypeInformation.of(Integer.class));
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> in) throws Exception {
                // 获取ValueState保存的中间结果
                Integer maxValue = mapState.get(in.f0);
                if (maxValue == null || in.f1 > maxValue) {
                    // 更新ValueState的中间结果
                    mapState.put(in.f0, in.f1);
                }
                return new Tuple2<>(in.f0, mapState.get(in.f0));
            }
        });
        maxDs.print();
        env.execute();
    }

}
