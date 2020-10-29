package com.chelsea.flink.stream.broadcaststate;

import java.util.List;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import com.chelsea.flink.stream.domain.SysUser;
import com.chelsea.flink.stream.service.SysUserService;
import com.chelsea.flink.stream.util.SpringUtil;

/**
 * 广播流
 * 
 * @author shevchenko
 *
 */
public class BroadcaststateDemo1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<SysUser> addSource = env.addSource(new MyParalleSourceFunction());
        MapStateDescriptor<String, String> mapStateDes = new MapStateDescriptor<>("broadcastState", String.class, String.class);
        BroadcastStream<SysUser> broadcast = addSource.broadcast(mapStateDes);
        env.execute();
    }
    
    static class MyParalleSourceFunction extends RichParallelSourceFunction<SysUser> {

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
        public void run(SourceContext<SysUser> out) throws Exception {
            while(isRunning) {
                List<SysUser> querySysUser = sysUserService.querySysUser();
                for(SysUser sysUser : querySysUser) {
                    out.collect(sysUser);
                }
                Thread.sleep(5000);
            }
        }
        
    }

}
