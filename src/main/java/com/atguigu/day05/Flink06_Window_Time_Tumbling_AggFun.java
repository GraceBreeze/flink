package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author GraceBreeze
 * @create 2022-08-24 22:24
 */
public class Flink06_Window_Time_Tumbling_AggFun {
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

        //TODO 6 对窗口中的数据做sum累加计算用Agg实现
        window.aggregate(new AggregateFunction<WaterSensor, Integer, Integer>() {
            //创建累加器
            @Override
            public Integer createAccumulator() {
                System.out.println("创建累加器。。。、");
                return 0;
            }

            //累加操作
            @Override
            public Integer add(WaterSensor waterSensor, Integer integer) {
                System.out.println("累加操作。。。");
                return waterSensor.getVc() + integer;
            }

            //通过累加器获取结果
            @Override
            public Integer getResult(Integer integer) {
                System.out.println("合并累加器。。。");
                return integer;
            }

            //合并累加器  主要用于窗口合并时调用合并累加器  主要用于会话窗口
            @Override
            public Integer merge(Integer integer, Integer acc1) {
                System.out.println("合并累加器。。。");
                return integer + acc1;
            }
        }).print();

        env.execute();


    }
}