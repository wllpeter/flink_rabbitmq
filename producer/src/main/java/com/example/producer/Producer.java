package com.example.producer;

import com.example.producer.vo.ApisMessageVo;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by 86131 on 2020/1/10.
 */
public class Producer {
    public final static String QUEUE_NAME = "peterMessage";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setPort(5672);

        Connection connection = factory.newConnection();
        //如果是string就不需要序列化了
        ApisMessageVo vo = new ApisMessageVo();
        vo.setCommunicationAddress("南京西路");
        vo.setCommunicationType("110");
        Gson gson = new Gson();
        String str = gson.toJson(vo);  //序列化
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        int num = 10;
        while (num-- > 0) {
            channel.basicPublish("", QUEUE_NAME, null, str.toString().getBytes("utf-8"));
        }
        //关闭通道和连接
        //channel.close();
        //connection.close();
    }
}
