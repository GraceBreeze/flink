package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author GraceBreeze
 * @create 2022-08-24 16:54
 */
public class Flink12_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3 将数据转为Json字符串
        SingleOutputStreamOperator<String> waterSensorJsonStream = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSON.toJSONString(waterSensor);
            }
        });

        //TODO 4 将数据写入Kafka
        waterSensorJsonStream.addSink(new FlinkKafkaProducer<String>("hadoop102:9092",
                "topic_sensor", new SimpleStringSchema()));

        env.execute();
    }
}