package com.chelsea.flink.stream.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 侧道输出demo，防止数据丢失
 * 
 * @author shevchenko
 *
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        // 在多并行度的情况下，watermark对齐会取所有线程中最小的watermark来作为当前水印
        // 如果并行度大于1但是只有1个线程处理数据，其他线程不能获取数据，故其他线程无法生成或更新水印，导致无法触发窗口计算
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // 设置处理时间为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 生成水印周期 默认200毫秒
        env.getConfig().setAutoWatermarkInterval(200);
        DataStreamSource<String> ds = env.socketTextStream("47.107.247.223", 9999);
        // 接收的字符串结构为 id,num,ts
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> map = ds.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple3<String, Integer, Long> map(String in) throws Exception {
                String[] words = in.split(",");
                return new Tuple3<>(words[0], Integer.valueOf(words[1]), Long.valueOf(words[2]));
            }
        });
        // 添加水印，设置水印允许的延迟
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarkDs =
                map.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(2)) {

                    private static final long serialVersionUID = 1L;

                    /**
                     * 设置ts作为eventtime字段
                     */
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> tuple) {
                        return tuple.f2;
                    }
                });
        // 定义侧道输出对象
        OutputTag<Tuple3<String, Integer, Long>> outputTag = new OutputTag<Tuple3<String, Integer, Long>>("lateData"){};
        // 设置窗口，5秒滚动
        WindowedStream<Tuple3<String, Integer, Long>, Tuple, TimeWindow> window =
                watermarkDs.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 在水印基础上再次增加允许延迟时间（再次触发窗口计算）
                .allowedLateness(Time.seconds(5))
                // 设置超过水印延迟时间和allowedLateness延迟时间的极端延迟数据接收对象
                .sideOutputLateData(outputTag);
        // 窗口计算
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> apply = window.apply(new WindowFunction<Tuple3<String, Integer, Long>, Tuple3<String, Integer, Long>, Tuple, TimeWindow>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> in, Collector<Tuple3<String, Integer, Long>> out) throws Exception {
                String id = null;
                Long ts = null;
                int num = 0;
                for (Tuple3<String, Integer, Long> tuple : in) {
                    num += tuple.f1;
                    id = tuple.f0;
                    ts = tuple.f2;
                }
                // 发送计算结果
                out.collect(new Tuple3<>(id, num, ts));
                // 获取窗口开始时间和结束时间
                System.out.println("窗口开始时间》》" + window.getStart() + "=====;窗口结束时间》》" + window.getEnd());
            }
        });
        apply.print();
        // 获取侧道输出的数据
        DataStream<Tuple3<String, Integer, Long>> sideOutput = apply.getSideOutput(outputTag);
        sideOutput.printToErr("侧道输出数据》》》");
        env.execute();
    }

}
