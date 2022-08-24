package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author GraceBreeze
 * @create 2022-08-23 23:27
 */
public class Flink07_TransForm_Union {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 创建一条字母流
        DataStreamSource<String> strSource = env.fromElements("a", "b", "c", "d");

        //创建一条字母流
        DataStreamSource<String> intSource = env.fromElements("1", "2", "3", "4");

        //创建一条字母流
        DataStreamSource<String> chSource = env.fromElements("你", "我", "他");

        //TODO 3 使用Union连接多条流
        DataStream<String> union = strSource.union(intSource, chSource);

        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value + "1111111";
            }
        }).print();

        env.execute();

    }
}