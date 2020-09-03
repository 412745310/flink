package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 统计单词个数
 * 
 * @author shevchenko
 *
 */
public class AggregateDemo {
    
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
        AggregateOperator<Tuple2<String, Integer>> aggregate = flatMap.groupBy(0).aggregate(Aggregations.SUM, 1);
        aggregate.print();
    }

}
