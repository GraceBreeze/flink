package com.atguigu.day04;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * @author GraceBreeze
 * @create 2022-08-24 19:18
 */
public class Flink01_Sink_Es {
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

        //TODO 4 将数据写入到Es
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));
        httpHosts.add(new HttpHost("hadoop103", 9200));
        httpHosts.add(new HttpHost("hadoop104", 9200));
        waterSensorJsonStream.addSink(new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                IndexRequest indexRequest = new IndexRequest("220309flink", "_doc", element.getId());
                String jsonStr = JSON.toJSONString(element);

                indexRequest.source(jsonStr, XContentType.JSON);
                indexer.add(indexRequest);
            }
        }).build());

        env.execute();
    }
}