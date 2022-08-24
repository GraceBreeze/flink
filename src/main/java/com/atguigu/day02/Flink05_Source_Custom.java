package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author GraceBreeze
 * @create 2022-08-23 21:04
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        //TODO 1 创建流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 通过自定义的Source源获取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource());

        streamSource.print();
        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private Boolean isRunning = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(new WaterSensor("sensor" + random.nextInt(1000),System.currentTimeMillis(),random.nextInt(500)));
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
