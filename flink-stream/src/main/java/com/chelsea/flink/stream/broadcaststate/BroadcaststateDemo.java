package com.chelsea.flink.stream.broadcaststate;

import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.chelsea.flink.stream.domain.SysUser;
import com.chelsea.flink.stream.service.SysUserService;
import com.chelsea.flink.stream.util.SpringUtil;

/**
 * 广播事件合并流处理demo
 * 
 * @author shevchenko
 *
 */
public class BroadcaststateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /** 设置事件流 start **/
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
        SingleOutputStreamOperator<Tuple2<String, String>> processDs = kafkaDs.process(new ProcessFunction<String, Tuple2<String, String>>() {

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
        /** 设置事件流 end **/
        /** 设置广播流 start **/
        DataStreamSource<Tuple2<String, String>> mySource = env.addSource(new MySourceFunction());
        MapStateDescriptor<String, String> mapStateDes = new MapStateDescriptor<>("broadcastState", String.class, String.class);
        BroadcastStream<Tuple2<String, String>> broadcastDs = mySource.broadcast(mapStateDes);
        /** 设置广播流 end **/
        // 两个流合并，原来流中的数据依然是独立存在
        BroadcastConnectedStream<Tuple2<String, String>, Tuple2<String, String>> connectDs = processDs.connect(broadcastDs);
        // 事件广播合并流处理
        SingleOutputStreamOperator<Tuple3<String, String, String>> process = connectDs.process(new MyBroadcastProcessFunction());
        process.print();
        env.execute();
    }
    
    /**
     * 自定义事件广播合并流处理类
     * 
     */
    static class MyBroadcastProcessFunction extends BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>> {

        private static final long serialVersionUID = 1L;
        private MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("broadcastState", TypeInformation.of(String.class), TypeInformation.of(String.class));

        /**
         * 处理广播流中的数据，可以修改广播流中的数据
         */
        @Override
        public void processBroadcastElement(
                Tuple2<String, String> in,
                BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>.Context ctx,
                Collector<Tuple3<String, String, String>> out) throws Exception {
            // 获取mapState对象
            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            // 将广播流数据存入mapState
            broadcastState.put(in.f0, in.f1);
        }

        /**
         * 处理事件流中的数据，广播流在这个方法中是只读，不能修改
         */
        @Override
        public void processElement(
                Tuple2<String, String> in,
                BroadcastProcessFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple3<String, String, String>>.ReadOnlyContext ctx,
                Collector<Tuple3<String, String, String>> out) throws Exception {
            // 获取mapState对象
            ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            // 判断mapState中是否包含事件流中的数据
            boolean contains = broadcastState.contains(in.f0);
            if (contains) {
                // 从mapState中获取数据
                String value = broadcastState.get(in.f0);
                out.collect(new Tuple3<>(in.f0, in.f1, value));
            }
        }
        
    }
    
    /**
     * 自定义mysql源
     * 
     */
    static class MySourceFunction extends RichSourceFunction<Tuple2<String, String>> {

        private static final long serialVersionUID = 1L;
        private boolean isRunning = true;
        private static final SysUserService sysUserService = SpringUtil.getInstance().getBean(SysUserService.class);
        
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化方法，每个task线程只执行一次");
        }
        
        @Override
        public void close() throws Exception {
            System.out.println("资源回收方法，每个task线程只执行一次");
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void run(SourceContext<Tuple2<String, String>> out) throws Exception {
            while(isRunning) {
                List<SysUser> querySysUser = sysUserService.querySysUser();
                for(SysUser sysUser : querySysUser) {
                    out.collect(new Tuple2<>(sysUser.getUsername(), sysUser.getAddress()));
                }
                Thread.sleep(5000);
            }
        }
        
    }

}
