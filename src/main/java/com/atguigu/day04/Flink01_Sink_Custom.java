package com.atguigu.day04;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author GraceBreeze
 * @create 2022-08-24 19:18
 */
public class Flink01_Sink_Custom {
    public static void main(String[] args) throws Exception {
        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3 将数据转为Json字符串
        SingleOutputStreamOperator<WaterSensor> waterSensorJsonStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        //TODO 4 自定义sink将数据写到Mysql
        waterSensorJsonStream.addSink(new MySink());

        env.execute();
    }

    public static class MySink extends RichSinkFunction<WaterSensor> {
        private PreparedStatement ps;
        private Connection conn;

        public void open(Configuration parameters) throws Exception {
            System.out.println("创建链接");
            //创建链接
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "123456");
            //语句预执行
            ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1, value.getId());
            ps.setLong(2, value.getTs());
            ps.setInt(3, value.getVc());
            ps.execute();
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭链接");
            ps.close();
            conn.close();
        }


    }
}