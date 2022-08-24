package com.atguigu.day03;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

/**
 * @author GraceBreeze
 * @create 2022-08-24 13:55
 */
public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3 使用process实现flatmap的功能，将数据转为Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneStream = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                  //  out.collect(Tuple2.of(word, 1));
                }

            }
        });

        //TODO 4 将相同的单词的数据聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneStream.keyBy(0);

        //TODO 5 使用process实现sum的功能
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            private Integer lastSum = 0;

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                lastSum++;
               // out.collect(Tuple2.of(ctx.getCurrentKey().toString(),lastSum));
            }
        }).print();

        env.execute();
    }
}