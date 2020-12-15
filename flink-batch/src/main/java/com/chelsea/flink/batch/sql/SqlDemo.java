package com.chelsea.flink.batch.sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import com.chelsea.flink.batch.domain.Order;

/**
 * sql批处理示例
 * 
 *
 */
public class SqlDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        DataSource<Order> orderDs = env.fromElements(new Order(1, "aaa", 111.1), new Order(2, "bbb", 222.1), new Order(3, "ccc", 333.1), new Order(
                4, "aaa", 444.1), new Order(5, "bbb", 555.1), new Order(6, "ccc", 666.1));
        // 将DataSet注册为一张表
        tableEnv.registerDataSet("t_order", orderDs);
        String sql = "select userName, sum(money) as sumMoney, max(money) maxMoney from t_order group by userName";
        Table query = tableEnv.sqlQuery(sql);
        // 打印查询结构
        query.printSchema();
        // 将查询结果转换为DataSet
        DataSet<Row> dataSet = tableEnv.toDataSet(query, Row.class);
        dataSet.print();
    }

}
