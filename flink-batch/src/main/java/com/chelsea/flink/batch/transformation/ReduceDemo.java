package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 统计单词个数
 * 
 * @author shevchenko
 *
 */
public class ReduceDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("hello world aa", "hello aa", "hello bb");
        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        // reduce：分组后的tuple集合先通过网络传输到下一个节点，再进行合并计算（tuple集合较大时性能低）
//        ReduceOperator<Tuple2<String, Integer>> reduce = flatMap.groupBy(0).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
//            
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
//                int sum = tuple1.f1 + tuple2.f1;
//                return new Tuple2<>(tuple1.f0, sum);
//            }
//        });
        
        // reduceGroup：分组后的tuple集合先在各自节点进行合并计算，再通过网络传输到下一个节点（tuple集合较大时性能高）
        GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> reduce = flatMap.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, Tuple2<String,Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void reduce(Iterable<Tuple2<String, Integer>> in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String key = "";
                int sum = 0;
                for (Tuple2<String, Integer> tuple : in) {
                    key = tuple.f0;
                    sum += tuple.f1;
                }
                out.collect(new Tuple2<>(key, sum));
            }});
        
//        AggregateOperator<Tuple2<String, Integer>> reduce = flatMap.groupBy(0).sum(1);
        
        reduce.print();
    }

}
