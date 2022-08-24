package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author GraceBreeze
 * @create 2022-08-23 19:58
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从集合中读取数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        DataStreamSource<Integer> streamSource = env.fromCollection(list);

        streamSource.print();
        env.execute();


    }
}