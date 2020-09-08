package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator.DefaultCross;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 笛卡尔积demo
 * 
 * @author shevchenko
 *
 */
public class CrossDemo {
    
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple2<String, Object>> userDs = env.fromElements(new Tuple2<>("1", "aa"), new Tuple2<>("2", "bb"), new Tuple2<>("3", "cc"));
        DataSource<Tuple2<String, Object>> cityDs = env.fromElements(new Tuple2<>("1", "cd"), new Tuple2<>("2", "sh"), new Tuple2<>("4", "ww"));
        DefaultCross<Tuple2<String, Object>, Tuple2<String, Object>> cross = userDs.cross(cityDs);
        MapOperator<Tuple2<Tuple2<String, Object>, Tuple2<String, Object>>, Tuple4<String, Object, String, Object>> map =
                cross.map(new MapFunction<Tuple2<Tuple2<String,Object>,Tuple2<String,Object>>, Tuple4<String,Object,String,Object>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple4<String, Object, String, Object> map(Tuple2<Tuple2<String, Object>, Tuple2<String, Object>> in)
                            throws Exception {
                        return new Tuple4<>(in.f0.f0, in.f0.f1, in.f1.f0, in.f1.f1);
                    }});
        map.returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)).print();
    }

}
