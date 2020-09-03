package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 单词去重
 * 
 * @author shevchenko
 *
 */
public class DistinctDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("hello world aa", "hello aa", "hello bb");
        FlatMapOperator<String, Tuple1<String>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple1<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String line, Collector<Tuple1<String>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple1<>(word));
                }
            }
        });
        flatMap.distinct().print();
    }

}
