package com.chelsea.flink.batch.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 数据源代码示例
 * 
 * @author shevchenko
 *
 */
public class DataSourceDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> ds = env.readTextFile("hdfs://172.18.20.237:9000/flink/batch/t_t1.txt");
        ds.print();
    }

}
