package com.chelsea.flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WordCountStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 在服务器中使用命令nc -lk 9999
        DataStream<String> ds = env.socketTextStream("47.107.247.223", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = in.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }});
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = timeWindow.sum(1);
        sum.print();
        env.execute();
    }

}
