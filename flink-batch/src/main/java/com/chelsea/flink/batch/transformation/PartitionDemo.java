package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * 分区策略
 * 
 * @author shevchenko
 *
 */
public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<Integer, Object>> ds = env.fromElements(new Tuple2<>(10, "aa"), new Tuple2<>(4, "aa"), new Tuple2<>(3, "cc"), new Tuple2<>(3, "dd"),
                new Tuple2<>(1, "dd"), new Tuple2<>(5, "gg"), new Tuple2<>(2, "dd"), new Tuple2<>(7, "yy"));
        
        // hash分区
        PartitionOperator<Tuple2<Integer, Object>> partitionByHash = ds.partitionByHash(1);
        partitionByHash.writeAsText("C:/Users/Administrator/Desktop/partitionByHash", WriteMode.OVERWRITE);
        
        // 范围分区
        PartitionOperator<Tuple2<Integer, Object>> partitionByRange = ds.partitionByRange(0);
        partitionByRange.writeAsText("C:/Users/Administrator/Desktop/partitionByRange", WriteMode.OVERWRITE);
        
        // 在hash分区的基础上对每个分区内的数据进行排序
        SortPartitionOperator<Tuple2<Integer, Object>> sortPartition = partitionByHash.sortPartition(0, Order.ASCENDING);
        sortPartition.writeAsText("C:/Users/Administrator/Desktop/sortPartition", WriteMode.OVERWRITE);
        
        env.execute();
    }

}
