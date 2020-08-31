package com.chelsea.flink.batch.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import com.chelsea.flink.batch.domain.Person;

public class MapAndMapPartition {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> ds = env.fromElements("1,aa", "2,bb", "3,cc", "4,dd", "5,ee", "6,ff");
        MapOperator<String, Person> map = ds.map(new MapFunction<String, Person>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Person map(String str) throws Exception {
                String[] strArr = str.split(",");
                String id = strArr[0];
                String name = strArr[1];
                Person person = new Person();
                person.setId(id);
                person.setName(name);
                return person;
            }
        });
        
        map.print();
    
        MapPartitionOperator<String, Person> mapPartition = ds.mapPartition(new MapPartitionFunction<String, Person>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void mapPartition(Iterable<String> in, Collector<Person> out) throws Exception {
                for (String str : in) {
                    String[] strArr = str.split(",");
                    String id = strArr[0];
                    String name = strArr[1];
                    Person person = new Person();
                    person.setId(id);
                    person.setName(name);
                    out.collect(person);
                }
            }
            
        });
        
        mapPartition.print();
    }
    
}
