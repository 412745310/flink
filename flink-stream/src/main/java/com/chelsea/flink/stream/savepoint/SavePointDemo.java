package com.chelsea.flink.stream.savepoint;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * 手动SavePoint示例
 *
 */
public class SavePointDemo {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启CK，指定使用FsStateBackend
        // 有三种backend：MemoryStateBackend,FsStateBackend,RocksdbStateBackend
        // MemoryStateBackend：用户开发测试
        // FsStateBackend：生产环境建议使用，并搭配hdfs
        // RocksdbStateBackend：生产环境使用，有超大状态保存时可以使用
        env.setStateBackend(new FsStateBackend("hdfs://172.18.20.237:9000/flink/stream/checkpoint"));
        // 这只checkpoint的周期间隔（毫秒），默认是没有开启ck，需要通过指定间隔来开启ck
        env.enableCheckpointing(1000);
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
        // 设置延迟重启策略，此处设置为程序出现异常的时候，重启3次，每次延迟5秒钟重启，超过3次，程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        DataStreamSource<String> mySourceDs = env.addSource(new MySource());
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapDs = mySourceDs.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = in.split(" ");
                for(int i = 0; i < words.length; i++) {
                    out.collect(new Tuple2<>(words[i], 1));
                }
            }
        });
        flatMapDs.keyBy(0).sum(1).print();
        env.execute();
    }

    /**
     * 自定义source
     *
     */
    static class MySource implements SourceFunction<String> {

        private static final long serialVersionUID = 1L;
        
        boolean flag = true;

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(flag) {
                ctx.collect("hello world");
                TimeUnit.SECONDS.sleep(1);
            }
        }

    }

}
