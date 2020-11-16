package com.chelsea.flink.stream.state;

import java.util.Iterator;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * 使用OperatorState存储中间值示例
 * 
 * @author shevchenko
 *
 */
public class OperatorStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
//        env.setStateBackend(new FsStateBackend("hdfs://47.107.247.223:9000/flink/stream/checkpoint"));
        // 设置checkpoint路径
        env.setStateBackend(new FsStateBackend("file:///C:/Users/Administrator/Desktop/checkpoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
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
