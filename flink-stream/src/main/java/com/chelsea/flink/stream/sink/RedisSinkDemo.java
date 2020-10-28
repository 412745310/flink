package com.chelsea.flink.stream.sink;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * 数据写入redis
 * 
 * @author shevchenko
 *
 */
public class RedisSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> ds = env.socketTextStream("47.107.247.223", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;
            
            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = in.split(" ");
                for(String w : words) {
                    out.collect(new Tuple2<>(w, 1));
                }
            }
            
        });
        // 先对元组的第一个字段分组，再对分组后的元组第二个字段求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> redDs = flatMap.keyBy(0).sum(1);
        Set<InetSocketAddress> nodes = new HashSet<>();
        nodes.add(new InetSocketAddress("127.0.0.1", 7001));
        nodes.add(new InetSocketAddress("127.0.0.1", 7002));
        nodes.add(new InetSocketAddress("127.0.0.1", 7003));
        nodes.add(new InetSocketAddress("127.0.0.1", 7004));
        nodes.add(new InetSocketAddress("127.0.0.1", 7005));
        nodes.add(new InetSocketAddress("127.0.0.1", 7006));
        // 设置redis连接
        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build();
        // 添加redis作为输出目标
        redDs.addSink(new RedisSink<Tuple2<String, Integer>>(config, new MyRedisMapper()));
        env.execute();
    }
    
    static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>> {

        private static final long serialVersionUID = 1L;

        /**
         * 设置redis结构，此处设置key类型为hash，key名为redissink
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "redissink");
        }

        /**
         * 设置元组第几个字段作为redis key/field进行存储
         */
        @Override
        public String getKeyFromData(Tuple2<String, Integer> tuple) {
            return tuple.f0;
        }

        /**
         * 设置元组第几个字段作为redis value进行存储
         */
        @Override
        public String getValueFromData(Tuple2<String, Integer> tuple) {
            return String.valueOf(tuple.f1);
        }
        
    }

}
