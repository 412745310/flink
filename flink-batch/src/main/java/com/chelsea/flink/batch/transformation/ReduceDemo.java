package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class ReduceDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("hello world", "hello aa", "hello bb");
        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        
        ReduceOperator<Tuple2<String, Integer>> reduce = flatMap.groupBy(0).reduce(new ReduceFunction<Tuple2<String,Integer>>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
                int sum = tuple1.f1 + tuple2.f1;
                return new Tuple2<>(tuple1.f0, sum);
            }
        });
        
//        AggregateOperator<Tuple2<String, Integer>> reduce = flatMap.groupBy(0).sum(1);
        
        reduce.print();
    }

}
