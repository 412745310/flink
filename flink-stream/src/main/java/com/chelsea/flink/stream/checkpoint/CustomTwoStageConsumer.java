package com.chelsea.flink.stream.checkpoint;

import java.util.Properties;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * 自定义两阶段提交消费端
 *
 */
public class CustomTwoStageConsumer {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
     // 开启CK，指定使用FsStateBackend
        // 有三种backend：MemoryStateBackend,FsStateBackend,RocksdbStateBackend
        // MemoryStateBackend：用户开发测试
        // FsStateBackend：生产环境建议使用，并搭配hdfs
        // RocksdbStateBackend：生产环境使用，有超大状态保存时可以使用
        env.setStateBackend(new FsStateBackend("file:///C:/Users/Administrator/Desktop/CustomTwoStageConsumer"));
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
        // 设置kafka的偏移量提交策略，此处设置为false，表示将提交策略交给checkpoint管理
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 获取kafkaConsumer对象
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("testTopic", new SimpleStringSchema(), prop);
        // 设置kafka消费者偏移量是基于CK成功时提交
        consumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> ds = env.addSource(consumer);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDs = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = in.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDs = flatMapDs.keyBy(0).sum(1);
        TypeSerializer<MyConnectionState> transactionSerializer = new KryoSerializer<>(MyConnectionState.class, new ExecutionConfig());
        TypeSerializer<Void> contextSerializer = VoidSerializer.INSTANCE;
        sumDs.addSink(new MyTwoPhaseCommit(transactionSerializer, contextSerializer));
        env.execute();
    }
    
    // 自定义sinkFunction实现两阶段提交
    static class MyTwoPhaseCommit extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>, MyConnectionState, Void> {

        private static final long serialVersionUID = 1L;
        
        // 初始化时执行，只执行一次
        public MyTwoPhaseCommit(TypeSerializer<MyConnectionState> transactionSerializer,
                TypeSerializer<Void> contextSerializer) {
            super(transactionSerializer, contextSerializer);
            System.out.println("构造方法，可以执行序列化定义");
        }

        // 回滚
        @Override
        protected void abort(MyConnectionState arg0) {
            System.out.println("回滚");
        }

        // 每次ck都会执行
        @Override
        protected MyConnectionState beginTransaction() throws Exception {
            System.out.println("开启事务");
            return null;
        }

        // 每次ck都会执行
        @Override
        protected void commit(MyConnectionState arg0) {
            System.out.println("真正提交");
        }

        // 只有真正接收到数据才执行
        @Override
        protected void invoke(MyConnectionState arg0, Tuple2<String, Integer> in, Context arg2) throws Exception {
            int i = 1/0;
            System.out.println("执行动作:" + in.f0 + "," + in.f1);
        }

        // 每次ck都会执行
        @Override
        protected void preCommit(MyConnectionState arg0) throws Exception {
            System.out.println("预提交");
        }
        
    }
    
    static class MyConnectionState {}

}
