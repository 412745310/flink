package com.chelsea.flink.stream.sql;

import java.util.Random;
import java.util.UUID;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import com.chelsea.flink.stream.domain.Order;

/**
 * sql流式处理demo
 *
 */
public class SqlDemo {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置处理时间为eventtime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 定义自定义数据源
        DataStreamSource<Order> orderDs = env.addSource(new MySource());
        // 添加水印，允许延迟2秒
        SingleOutputStreamOperator<Order> wmDs = orderDs.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(2)) {

            private static final long serialVersionUID = 1L;

            /**
             * 设置ts作为eventtime字段
             */
            @Override
            public long extractTimestamp(Order order) {
                return order.getCreateTime();
            }
        });
        // 将DataStream注册为表
        tableEnv.registerDataStream("t_order", wmDs, "orderId,userId,money,createTime.rowTime");
        String sql = "select userId, sum(money) as sumMoney, max(money) maxMoney from t_order group by tumble(createTime, interval '5' seconds), userId";
        Table query = tableEnv.sqlQuery(sql);
        tableEnv.toRetractStream(query, Row.class).print();
        env.execute();
    }

    static class MySource extends RichSourceFunction<Order> {

        private static final long serialVersionUID = 1L;

        boolean flag = true;

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            while(flag) {
                ctx.collect(new Order(UUID.randomUUID().toString(), (new Random()).nextInt(3), (new Random()).nextInt(101),
                        System.currentTimeMillis()));
                Thread.sleep(1000);
            }
        }
    }

}
