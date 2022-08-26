package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author GraceBreeze
 * @create 2022-08-25 21:10
 */
public class Flink02_ProcessTime_Timer {
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
        });

        //TODO 4 将相同key的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //TODO 5 注册基于处理时间的定时器，定时5s，5s之后定时器触发，打印一句话
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                System.out.println((ctx.timerService().currentProcessingTime())/1000);
                //1.注册一个基于处理时间的定时器
                ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
            }


            @Override
            public void onTimer(long timestamp, OnTimerContext ctx,Collector<String> out) {
                System.out.println("触发后：" + ctx.timerService().currentProcessingTime()/1000);
                out.collect("定时器已被触发");
            }
        }).print();


        env.execute();
    }



}