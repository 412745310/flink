package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 连接示例
 * 
 * @author shevchenko
 *
 */
public class JoinDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<String, Object>> userDs = env.fromElements(new Tuple2<>("1", "aa"), new Tuple2<>("2", "bb"), new Tuple2<>("3", "cc"));
        DataSource<Tuple2<String, Object>> cityDs = env.fromElements(new Tuple2<>("1", "cd"), new Tuple2<>("2", "sh"), new Tuple2<>("4", "ww"));
        // 内连接
        System.out.println("================内连接结果==============");
        userDs.join(cityDs, JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0).print();
        // 左连接
        JoinFunctionAssigner<Tuple2<String, Object>, Tuple2<String, Object>> equalTo = userDs.leftOuterJoin(cityDs, JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0);
        JoinOperator<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>> joinOperator = equalTo.with(new FlatJoinFunction<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void join(Tuple2<String, Object> tuple1, Tuple2<String, Object> tuple2, Collector<Tuple2<String, Object>> out) throws Exception {
                if (tuple2 == null) {
                    out.collect(new Tuple2<String, Object>((String) tuple1.f1, null));
                } else {
                    out.collect(new Tuple2<String, Object>((String) tuple1.f1, tuple2.f1));
                }
            }});
        System.out.println("================左连接结果==============");
        joinOperator.returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
        // 右连接
        JoinFunctionAssigner<Tuple2<String, Object>, Tuple2<String, Object>> equalTo2 = userDs.rightOuterJoin(cityDs, JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0);
        JoinOperator<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>> with = equalTo2.with(new FlatJoinFunction<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void join(Tuple2<String, Object> tuple1, Tuple2<String, Object> tuple2, Collector<Tuple2<String, Object>> out) throws Exception {
                if (tuple1 == null) {
                    out.collect(new Tuple2<String, Object>((String) tuple2.f1, null));
                } else {
                    out.collect(new Tuple2<String, Object>((String) tuple2.f1, tuple1.f1));
                }
            }});
        System.out.println("================右连接结果==============");
        with.returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
        // 全连接
        JoinFunctionAssigner<Tuple2<String, Object>, Tuple2<String, Object>> equalTo3 = userDs.fullOuterJoin(cityDs, JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0);
        JoinOperator<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>> with2 = equalTo3.with(new FlatJoinFunction<Tuple2<String, Object>, Tuple2<String, Object>, Tuple2<String, Object>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void join(Tuple2<String, Object> tuple1, Tuple2<String, Object> tuple2, Collector<Tuple2<String, Object>> out) throws Exception {
                if (tuple1 == null) {
                    out.collect(new Tuple2<String, Object>(null, tuple2.f1));
                } else if (tuple2 == null) {
                    out.collect(new Tuple2<String, Object>((String) tuple1.f1, null));
                } else {
                    out.collect(new Tuple2<String, Object>((String) tuple1.f1, tuple2.f1));
                }
            }});
        System.out.println("================全连接结果==============");
        with2.returns(Types.TUPLE(Types.STRING, Types.STRING)).print();
        // union操作
        System.out.println("================union结果==============");
        userDs.union(cityDs).print();
    }
    
}
