package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author GraceBreeze
 * @create 2022-08-25 10:44
 */
public class Flink03_CountWindow_Tumbling {
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
                System.out.println("数据来的时间：" + System.currentTimeMillis()/1000);
                String[] split = value.split(",");
                return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
            }
        });

        //TODO 4 将相同的id聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("id");

        //TODO 5 开启一个基于元素个数的滚动窗口
        WindowedStream<WaterSensor, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(5);

        countWindow.sum("vc").print();

        env.execute();
    }
}

