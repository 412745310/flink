package com.chelsea.flink.stream.broadcaststate;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 广播流demo
 * 
 * @author shevchenko
 *
 */
public class BroadcaststateDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        // kafka集群
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 消费组
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        // 动态分区检测
        prop.setProperty("flink.partition-discovery.interval-millis", "5000");
        // 设置kv的反序列化使用的类
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置默认消费的偏移量起始值（从最新处消费）
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 获取kafkaConsumer对象
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("testTopic", new SimpleStringSchema(), prop);
        DataStreamSource<String> kafkaDs = env.addSource(consumer);
        // 使用process转换事件流，用法类似map，但比map功能更强大
        SingleOutputStreamOperator<Tuple2<String, String>> process = kafkaDs.process(new ProcessFunction<String, Tuple2<String, String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void processElement(String in, ProcessFunction<String, Tuple2<String, String>>.Context ctx,
                    Collector<Tuple2<String, String>> out) throws Exception {
                String[] words = in.split(" ");
                String username = words[0];
                String address = words[1];
                out.collect(new Tuple2<>(username, address));
            }
            
        });
        process.print();
        env.execute();
    }

}
