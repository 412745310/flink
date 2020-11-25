package com.chelsea.flink.stream.checkpoint;

import java.util.concurrent.TimeUnit;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * checkpoint设置demo
 *
 */
public class CheckpointDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启CK，指定使用FsStateBackend
        // 有三种backend：MemoryStateBackend,FsStateBackend,RocksdbStateBackend
        // MemoryStateBackend：用户开发测试
        // FsStateBackend：生产环境建议使用，并搭配hdfs
        // RocksdbStateBackend：生产环境使用，有超大状态保存时可以使用
        env.setStateBackend(new FsStateBackend("hdfs://47.107.247.223:9000/flink/stream/checkpoint1"));
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
        // 设置自定义source
        DataStreamSource<String> ds = env.addSource(new MySource());
        ds.print();
        env.execute();
    }
    
    static class MySource extends RichSourceFunction<String> {
        
        private static final long serialVersionUID = 1L;
        private boolean flag = true;

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<String> ctx)
                throws Exception {
            while(flag) {
                ctx.collect("hello flink");
                TimeUnit.SECONDS.sleep(1);
            }
        }
        
    }

}
