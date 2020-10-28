package com.chelsea.flink.stream.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.chelsea.flink.stream.domain.SysUser;
import com.chelsea.flink.stream.service.SysUserService;
import com.chelsea.flink.stream.util.SpringUtil;

/**
 * 数据写入mysql
 * 
 * @author shevchenko
 *
 */
public class MysqlSink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStreamSource<SysUser> ds = env.fromElements(new SysUser("aaa", "aaa", "sichuan"));
        ds.addSink(new MysqlSinkFunction());
        env.execute();
    }

    static class MysqlSinkFunction extends RichSinkFunction<SysUser> {

        private static final long serialVersionUID = 1L;
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
        public void invoke(SysUser value) throws Exception {
            sysUserService.addSysUser(value);
        }
    }

}
