package com.chelsea.flink.batch.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

public class FlatMapAndFilter {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<String> list = Arrays.asList("aaaaa", "bbb", "ccccc", "dddd");
        DataSource<String> ds = env.fromCollection(list);
        FlatMapOperator<String, String> flatMap = ds.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String in, Collector<String> out) throws Exception {
                out.collect(in);
                out.collect(in + ":" + 1);
            }
            
        });
        flatMap.print();
        System.out.println("==============================");
        FilterOperator<String> filter = flatMap.filter(new FilterFunction<String>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(String in) throws Exception {
                return in.length() > 4;
            }
        });
        filter.print();
    }

}
