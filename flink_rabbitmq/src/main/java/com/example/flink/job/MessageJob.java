package com.example.flink.job;

import com.example.flink.mysql.SinkMysql;
import com.example.flink.vo.ApisMessageVo;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * Created by 86131 on 2020/1/10.
 */
public class MessageJob {


    public static void main(String[] args) throws Exception {
        //1.获取flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672)
                .setUserName("admin")
                .setPassword("admin")
                .setVirtualHost("/")
                .build();

        //2.连接socket获取输入的数据(数据源Data Source)
        //2.1. rabbitmq连接的配置，2.rabbitmq的队列名，消费的队列名
        DataStream<String> dataStreamSource = env.addSource(new RMQSource<String>(connectionConfig,
                "peterMessage", true, new SimpleStringSchema()));
        dataStreamSource.print();   //输出Source的信息
        //3.数据转换
        //MapFunction:第一个参数是你接收的数据的类型
        //MapFunction:第二个参数是返回数据的类型
        DataStream<ApisMessageVo> receiveMessageVo =
                dataStreamSource.map(new
                                             MapFunction<String, ApisMessageVo>() {
                                                 public ApisMessageVo map(String value) throws Exception {
                                                     Gson gson = new Gson();
                                                     //反序列化,拿到实体(我这里接受的是实体，如果你只是string 直接return就完事了)
                                                     ApisMessageVo apisMessageVo = gson.fromJson(value, ApisMessageVo.class);
                                                     return apisMessageVo;
                                                 }
                                             });
        //4.sink输出
        receiveMessageVo.addSink(new SinkMysql());
        //5.这一行代码一定要实现，否则程序不执行
        env.execute("Socket window count");
    }

}
