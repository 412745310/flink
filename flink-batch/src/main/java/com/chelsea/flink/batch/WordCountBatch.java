package com.chelsea.flink.batch;

import lombok.extern.slf4j.Slf4j;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 单词统计
 * 
 * @author shevchenko
 *
 */
public class WordCountBatch {
    
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 加载或创建源数据
        DataSet<String> text = env.fromElements("this a book", "i love china", "i am chinese");

        // 转化处理数据
        FlatMapOperator<String, Tuple2<String, Integer>> ds = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> result = ds.groupBy(0).sum(1);
        for(Tuple2<String, Integer> tuple : result.collect()) {
            System.out.println(tuple.f0 + "," + tuple.f1);
        }
    }

}
