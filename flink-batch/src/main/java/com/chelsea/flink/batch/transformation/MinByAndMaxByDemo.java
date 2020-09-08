package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 求最小值和最大值
 * 
 * @author shevchenko
 *
 */
public class MinByAndMaxByDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Tuple3<String, String, Integer>> ds = env.fromElements(new Tuple3<>("aa", "yuwen", 80), new Tuple3<>("aa", "shuxue", 90), new Tuple3<>("aa", "yingyu",
                80), new Tuple3<>("bb", "yuwen", 90), new Tuple3<>("bb", "shuxue", 70), new Tuple3<>("bb", "yingyu", 80),
                new Tuple3<>("cc", "yuwen", 85), new Tuple3<>("cc", "shuxue", 100), new Tuple3<>("cc", "yingyu", 70));
        UnsortedGrouping<Tuple3<String, String, Integer>> groupBy = ds.groupBy(1);
        System.out.println("================min==================");
        groupBy.min(2).print();
        System.out.println("================minby==================");
        groupBy.minBy(2).print();
        System.out.println("================max==================");
        groupBy.max(2).print();
        System.out.println("================maxBy==================");
        groupBy.maxBy(2).print();
    }

}
