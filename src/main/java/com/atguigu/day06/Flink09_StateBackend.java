package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author GraceBreeze
 * @create 2022-08-26 20:29
 */
public class Flink09_StateBackend {
    public static void main(String[] args) throws IOException {

        //TODO 1 获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 设置状态后端
        //老版本配置方式
        // 内存级别
        env.setStateBackend(new MemoryStateBackend());

        //文件系统级别
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/ck"));

        //Rocks DB
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/rocksdb"));


    }
}