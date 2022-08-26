package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author GraceBreeze
 * @create 2022-08-25 21:10
 */
public class Flink03_EventTime_Timer {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3 将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>(){
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000;
                    }
                })
                );

        //TODO 4 将相同key的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //TODO 5 注册基于事件时间的定时器，定时5s，5s之后定时器触发，打印一句话
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.注册一个基于事件时间的定时器
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000);
            }


            @Override
            public void onTimer(long timestamp, OnTimerContext ctx,Collector<String> out) {
                System.out.println("定时器触发：" + timestamp);
                System.out.println("定时器触发：" + ctx.timestamp());
                out.collect("已被触发");
            }
        }).print();


        env.execute();
    }



}