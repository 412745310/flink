package com.chelsea.flink.stream.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流split demo
 * @author shevchenko
 *
 */
public class SplitDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds = env.fromCollection(Arrays.asList(1,2,3,4,5,6));
        SplitStream<Integer> split = ds.split(new OutputSelector<Integer>() {
            
            private static final long serialVersionUID = 1L;
            
            @Override
            public Iterable<String> select(Integer in) {
                List<String> out = new ArrayList<>();
                int res = in % 2;
                if (res == 0) {
                    out.add("even");
                } else {
                    out.add("odd");
                }
                return out;
            }
        });
        // 获取偶数
        DataStream<Integer> even = split.select("even");
        // 获取奇数
        DataStream<Integer> odd = split.select("odd");
        // 获取所有
        DataStream<Integer> all = split.select("even","odd");
        even.print();
//        odd.print();
//        all.print();
        env.execute();
    }

}
