package com.chelsea.flink.stream.checkpoint;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * 两阶段提交demo
 *
 */
public class TwoStageSubmit {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启CK，指定使用FsStateBackend
        // 有三种backend：MemoryStateBackend,FsStateBackend,RocksdbStateBackend
        // MemoryStateBackend：用户开发测试
        // FsStateBackend：生产环境建议使用，并搭配hdfs
        // RocksdbStateBackend：生产环境使用，有超大状态保存时可以使用
        env.setStateBackend(new FsStateBackend("file:///C:/Users/Administrator/Desktop/checkpoint"));
        // checkpoint的周期间隔（毫秒），默认是没有开启ck，需要通过指定间隔来开启ck
        // 表示每隔10秒进行一次checkpoint，在所有operation未全部进行checkpoint时，向sink下发的数据都是未提交状态
        env.enableCheckpointing(10000);
        // 设置ck的执行语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两次ck之间的最小间隔（毫秒）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置ck超时时间（毫秒），如果超时则认为本次ck失败，继续下一次ck
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置ck出现问题时，是否让程序继续执行，true：程序报错，false：不报错继续下次ck
        // 如果设置为false，下次ck成功则没有问题，如果下次ck也失败，此时如果程序挂掉需要从ck恢复数据时，可能导致计算错误或是重复计算等问题
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // 设置任务取消时是否保留检查点，retain：保留，delete：删除作业数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置同时最多允许几个ck同时执行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 读取socket数据
        DataStreamSource<String> socketDs = env.socketTextStream("47.107.247.223", 9999);
        // 打印
        socketDs.print();
        // 输出到kafka
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 设置kafka事务超时时间（毫秒）
        // 默认FlinkKafkaProducer事务超时时间是1小时，而kafka事务超时时间是15分钟，可能会造成数据丢失
        // 尽量保持FlinkKafkaProducer和kafka事务超时时间一致，此处也设置为15分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000 * 15 + "");
        KeyedSerializationSchemaWrapper<String> wrapper = new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema());
        // 设置producer语义，默认是at-least-once
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer("testTopic", wrapper, prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        socketDs.addSink(kafkaSink);
        env.execute();
    }

}
