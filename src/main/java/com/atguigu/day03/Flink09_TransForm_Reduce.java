package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author GraceBreeze
 * @create 2022-08-23 23:09
 */
public class Flink09_TransForm_Reduce {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3 使用Map将端口过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream= streamSource.map(new MapFunction<String, WaterSensor>(){
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }
        });

        //TODO 4 将相同id的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //TODO 5 使用ruduce求Vc的最大值
        keyedStream.reduce(new ReduceFunction<WaterSensor>() {

            /*
            * value1 上一次聚合后的结果（历史数据）
            * value2 当前进来的数据
            * */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(),value1.getTs(),Math.max(value1.getVc(),value2.getVc()));
            }
        }).print();

        env.execute();

    }
}