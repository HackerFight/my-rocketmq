package com.qiuguan.rocketmq.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author: qiuguan
 * date: 2022/2/13 - 下午8:50
 *
 * rocketmq 生产者同步发送消息
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {

        System.out.println(Runtime.getRuntime().availableProcessors());

        DefaultMQProducer producer = new DefaultMQProducer("qiuguan_group");
        producer.setNamesrvAddr("47.96.111.179:9876");
        producer.start();

        for (int i = 0; i < 1 ; i++) {
            byte[] body = ("hi:" + i).getBytes();
            Message message = new Message("qiuguan_topic", "qiuguan_tag", body);
            SendResult send = producer.send(message, 10000);
            System.out.println("sendResult: " + send);
        }

        producer.shutdown();
    }
}
