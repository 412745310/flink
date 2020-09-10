package com.chelsea.flink.batch.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/**
 * 累加器使用
 * 
 * @author shevchenko
 *
 */
public class AccumulatorDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("hello world aa", "hello aa", "hello bb");
        MapPartitionOperator<String, String> mapPartition = ds.setParallelism(3).mapPartition(new RichMapPartitionFunction<String, String>() {

            private static final long serialVersionUID = 1L;
            
            private IntCounter count = new IntCounter();
            
            private int i = 0;
            
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("---------初始化累加器---------");
                getRuntimeContext().addAccumulator("wordcount", count);
            }

            @Override
            public void mapPartition(Iterable<String> in, Collector<String> out) throws Exception {
                System.out.println("---------分区内运算---------");
                for (String s: in) {
                    int length = s.split(" ").length;
                    count.add(length);
                    i += length;
                }
                System.out.println("---------分区内计数: " + i + " ---------");
            }
        });
        mapPartition.writeAsText("C:/Users/Administrator/Desktop/accumulator", WriteMode.OVERWRITE);
        JobExecutionResult execute = env.execute();
        Object accumulatorResult = execute.getAccumulatorResult("wordcount");
        System.out.println("---------各分区总计数: " + accumulatorResult + " ---------");
    }
}
