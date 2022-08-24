package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author GraceBreeze
 * @create 2022-08-22 21:28
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 2 读取文件
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //TODO 3 flatMap切分每一个单词
        FlatMapOperator<String, String> word = dataSource.flatMap(new MyFlatMap());

        //TODO 4 Map将每一个单词组成Tuple2元组
        MapOperator<String, Tuple2<String,Integer>> wordToOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value,1);
            }
        });

        //TODO 5 将相同的单词聚合到一起
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //TODO 6 做累加计算
        AggregateOperator<Tuple2<String, Integer>> sum = groupBy.sum(1);

        //TODO 7打印结果
        sum.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String>{
        @Override
        public void flatMap(String value, Collector<String> out)
        {
            //1.将数据按照空格切分
            String[] words = value.split(" ");

            //2.遍历字符串数组，获取到每一个单词
            for (String word: words) {
                //将每一个单词发送至下游
                out.collect(word);
            }
        }
    }
}