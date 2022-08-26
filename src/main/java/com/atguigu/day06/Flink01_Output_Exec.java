package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author GraceBreeze
 * @create 2022-08-25 21:10
 */
public class Flink01_Output_Exec {
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


        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //采集监控水位器水位值，将水位高于5cm和低于2cm的值发送到side output
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //将水位大于5cm的数据再发送一份到测输出流一份
                if (value.getVc() > 5) {
                    ctx.output(new OutputTag<WaterSensor>("up5com") {
                    }, value);
                }
                //水位小于2cm的放到另一个测输出流中
                else if (value.getVc() < 2) {
                    ctx.output(new OutputTag<WaterSensor>("low2cm") {
                    }, value);
                }
                //再把所有的数据发送至主流
                out.collect(value);
            }
        });

        process.print("主流");
        DataStream<WaterSensor> up5com = process.getSideOutput(new OutputTag<WaterSensor>("up5com"){});
        up5com.print("高于5cm");

        DataStream<WaterSensor> low2cm = process.getSideOutput(new OutputTag<WaterSensor>("low2cm"){});
        low2cm.print("低于2cm");


        env.execute();
    }



}