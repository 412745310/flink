package com.chelsea.flink.stream.restart;

import java.util.Iterator;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 重启策略设置demo
 * 
 * @author shevchenko
 *
 */
public class RestartDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启CK，指定使用FsStateBackend
        // 有三种backend：MemoryStateBackend,FsStateBackend,RocksdbStateBackend
        // MemoryStateBackend：用户开发测试
        // FsStateBackend：生产环境建议使用，并搭配hdfs
        // RocksdbStateBackend：生产环境使用，有超大状态保存时可以使用
        env.setStateBackend(new FsStateBackend("file:///C:/Users/Administrator/Desktop/checkpoint"));
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
        env.addSource(new MySource()).print();
        env.execute();
    }

    /**
     * 自定义source，如果要使用OperatorState，必须要实现CheckpointedFunction接口
     * @author shevchenko
     *
     */
    static class MySource implements SourceFunction<String>, CheckpointedFunction {

        private static final long serialVersionUID = 1L;
        
        boolean flag = true;
        long offset = 0l;
        ListState<Long> listState = null;

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void run(SourceContext<String> context) throws Exception {
            while(flag) {
                // 从ListState中获取之前存储的offset
                Iterator<Long> iterator = listState.get().iterator();
                if (iterator.hasNext()) {
                    offset = iterator.next();
                }
                offset += 1;
                context.collect(offset + "");
                System.out.println("当前偏移量 : " + offset);
                Thread.sleep(1000);
                if (offset % 5 == 0) {
                    System.out.println("程序出现异常，自动重启中.....");
                    throw new RuntimeException("程序异常");
                }
            }
        }

        /**
         * 初始化方法，初始化OperatorState对象
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Long> listStateDescriptor = new ListStateDescriptor<>("listState", Long.class);
            listState = context.getOperatorStateStore().getListState(listStateDescriptor);
        }

        /**
         * 每次checkpoint时会调用这个方法
         */
        @Override
        public void snapshotState(FunctionSnapshotContext arg0) throws Exception {
            // 把ListState清空
            listState.clear();
            // 把最新的offset加入到ListState
            listState.add(offset);
        }

    }

}
