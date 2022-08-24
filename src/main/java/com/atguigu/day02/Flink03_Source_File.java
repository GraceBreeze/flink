package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author GraceBreeze
 * @create 2022-08-23 20:11
 */
public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt").setParallelism(2);

        streamSource.print();
        env.execute();
    }
}