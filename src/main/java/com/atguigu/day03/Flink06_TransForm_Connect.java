package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author GraceBreeze
 * @create 2022-08-23 23:27
 */
public class Flink06_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 创建一条字母流
        DataStreamSource<String> strSource = env.fromElements("a", "b", "c", "d");

        //创建一条数字流
        DataStreamSource<Integer> intSource = env.fromElements(1, 2, 3, 4);

        //TODO 3 使用Connect连接两条流
        ConnectedStreams<String, Integer> connect = strSource.connect(intSource);

        //对连接后的流做Map
        connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "aaaa";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * value + "";
            }
        }).print();

        env.execute();

    }
}