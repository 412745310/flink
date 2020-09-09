package com.chelsea.flink.batch.sink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * sink输出示例
 * 
 * @author shevchenko
 *
 */
public class SinkDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("hello world aa", "hello aa", "hello bb");
        // 未设置并行度，结果输出到1个文件里
//        ds.writeAsText("C:/Users/Administrator/Desktop/data", WriteMode.OVERWRITE);
        // 并行度设置为1，结果输出到1个文件里
//        ds.setParallelism(1).writeAsText("C:/Users/Administrator/Desktop/data1", WriteMode.OVERWRITE);
        // 并行度设置为2，结果输出到2个文件里
        ds.setParallelism(2).writeAsText("C:/Users/Administrator/Desktop/data2", WriteMode.OVERWRITE);
        env.execute();
    }

}
