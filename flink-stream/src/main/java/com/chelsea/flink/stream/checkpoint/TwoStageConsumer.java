package com.chelsea.flink.stream.checkpoint;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 两阶段提交消费端
 *
 */
public class TwoStageConsumer {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        // kafka集群
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 消费组
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testTopic");
        // 动态分区检测
        prop.setProperty("flink.partition-discovery.interval-millis", "5000");
        // 设置kv的反序列化使用的类
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 设置默认消费的偏移量起始值（从最新处消费）
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 设置消费者的隔离级别，默认是读取未提交数据
        // 此处设置为读取已提交数据，也就是两阶段提交之后的数据
        prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // 获取kafkaConsumer对象
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("testTopic", new SimpleStringSchema(), prop);
        DataStreamSource<String> ds = env.addSource(consumer);
        ds.print();
        env.execute();
    }

}
