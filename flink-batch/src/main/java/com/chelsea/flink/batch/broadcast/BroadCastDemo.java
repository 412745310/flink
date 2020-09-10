package com.chelsea.flink.batch.broadcast;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 广播变量demo
 * 
 * @author shevchenko
 *
 */
public class BroadCastDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<String, Object>> userDs =
                env.fromElements(new Tuple2<>("1", "aa"), new Tuple2<>("2", "bb"), new Tuple2<>("3", "cc"));
        DataSource<Tuple2<String, Object>> cityDs =
                env.fromElements(new Tuple2<>("1", "cd"), new Tuple2<>("2", "sh"), new Tuple2<>("4", "ww"));
        MapPartitionOperator<Tuple2<String, Object>, Tuple2<String, Object>> withBroadcastSet = userDs.mapPartition(new RichMapPartitionFunction<Tuple2<String,Object>, Tuple2<String, Object>>() {

            private static final long serialVersionUID = 1L;
            
            private Map<String, Object> cityDsMap = new HashMap<>();
            
            /**
             * 初始化方法，只会执行一次
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                List<Object> broadcastVariable = getRuntimeContext().getBroadcastVariable("cityDs");
                for (Object obj : broadcastVariable) {
                    Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
                    cityDsMap.put(tuple.f0, tuple.f1);
                }
            }

            @Override
            public void mapPartition(Iterable<Tuple2<String, Object>> in, Collector<Tuple2<String, Object>> out)
                    throws Exception {
                for (Tuple2<String, Object> tuple : in) {
                    Object object = cityDsMap.get(tuple.f0);
                    if (object == null) {
                        continue;
                    }
                    out.collect(new Tuple2<>(String.valueOf(tuple.f1), object));
                }
            }}).withBroadcastSet(cityDs, "cityDs");
        
        withBroadcastSet.returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
    }

}
