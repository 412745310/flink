package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.operators.SingleInputOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 重分区示例类
 * 解决不同分区之间的数据倾斜问题
 * 
 * @author shevchenko
 *
 */
public class RebalanceDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Long> ds = env.generateSequence(0, 100);
        FilterOperator<Long> filter = ds.filter(new FilterFunction<Long>() {
            
            private static final long serialVersionUID = 1L;

            @Override
            public boolean filter(Long in) throws Exception {
                return in > 20;
            }
        });
        GroupReduceOperator mapAndReduce = MapAndReduce(filter);
        System.out.println("==============rebalance之前================");
        mapAndReduce.print();
        PartitionOperator<Long> rebalance = filter.rebalance();
        GroupReduceOperator mapAndReduce2 = MapAndReduce(rebalance);
        System.out.println("==============rebalance之后================");
        mapAndReduce2.print();
    }
    
    private static GroupReduceOperator MapAndReduce(SingleInputOperator filter) {
        MapOperator<Long, Tuple2<Integer, Long>> map = filter.map(new RichMapFunction<Long, Tuple2<Integer, Long>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Long> map(Long in) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return new Tuple2<>(index, in);
            }});
        GroupReduceOperator<Tuple2<Integer, Long>, Tuple2<Integer, Long>> reduceGroup = map.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer,Long>, Tuple2<Integer,Long>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void reduce(Iterable<Tuple2<Integer, Long>> in, Collector<Tuple2<Integer, Long>> out)
                    throws Exception {
                Integer key = null;
                Long count = 0l;
                for (Tuple2<Integer, Long> tuple : in) {
                    key = tuple.f0;
                    count++;
                }
                out.collect(new Tuple2<>(key, count));
            }});
        return reduceGroup;
    }

}
