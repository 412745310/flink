package com.chelsea.flink.batch.distributeCache;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import cn.hutool.core.io.file.FileReader;

/**
 * 分布式缓存demo
 * 
 * @author shevchenko
 *
 */
public class DistributeCacheDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("C:/Users/Administrator/Desktop/city.txt", "cache_city");
        DataSource<Tuple2<String, Object>> userDs =
                env.fromElements(new Tuple2<>("3", "aa"), new Tuple2<>("2", "bb"), new Tuple2<>("1", "cc"));
        MapPartitionOperator<Tuple2<String, Object>, Tuple2<String, Object>> withBroadcastSet = userDs.mapPartition(new RichMapPartitionFunction<Tuple2<String,Object>, Tuple2<String, Object>>() {

            private static final long serialVersionUID = 1L;
            
            private Map<String, Object> cityDsMap = new HashMap<>();
            
            /**
             * 初始化方法，只会执行一次
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("cache_city");
                FileReader fileReader = new FileReader(file.getAbsolutePath());
                String content = fileReader.readString();
                List<String> lines = Arrays.asList(content.split("\n"));
                for (String line : lines) {
                    if (StringUtils.isBlank(line)) {
                        continue;
                    }
                    String[] words = line.split(",");
                    if (words.length != 2) {
                        continue;
                    }
                    cityDsMap.put(words[0], words[1]);
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
            }});
        withBroadcastSet.returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
    }

}
