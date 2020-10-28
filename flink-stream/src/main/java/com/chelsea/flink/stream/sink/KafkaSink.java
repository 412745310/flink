package com.chelsea.flink.stream.sink;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.chelsea.flink.stream.domain.SysUser;

/**
 * 数据写入kafka
 * 
 * @author shevchenko
 *
 */
public class KafkaSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SysUser> ds = env.fromElements(new SysUser("aaa", "aaa", "sichuan"));
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        KeyedSerializationSchemaWrapper<String> wrapper = new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema());
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("testTopic", wrapper, prop);
        SingleOutputStreamOperator<String> map = ds.map(new MapFunction<SysUser, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String map(SysUser in) throws Exception {
                return in.toString();
            }
            
        });
        map.addSink(kafkaSink);
        env.execute();
    }

}
