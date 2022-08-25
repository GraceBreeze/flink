package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author GraceBreeze
 * @create 2022-08-24 22:24
 */
public class Flink07_Window_Time_Tumbling_ProcessFun {
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

        //TODO 5 开启一个基于元素个数的滑动窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //TODO 6 对窗口中的数据做sum累加计算用Process实现（全窗口）
        window.process(new ProcessWindowFunction<WaterSensor, Tuple2<String,Integer>, Tuple, TimeWindow>() {
            private Integer lastSum = 0;

            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
                System.out.println("process ...");
                for (WaterSensor element : elements) {
                    lastSum += element.getVc();
                }
                out.collect(Tuple2.of(tuple.toString(), lastSum));
            }
        }).print();


        env.execute();


    }
}