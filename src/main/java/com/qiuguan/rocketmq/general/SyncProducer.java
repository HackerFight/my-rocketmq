package com.qiuguan.rocketmq.general;

import com.qiuguan.rocketmq.util.MQConstant;
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
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.start();

        for (int i = 0; i < 1 ; i++) {
            byte[] body = ("hi qiuguan, welcome to china:" + i).getBytes();
            Message message = new Message("qiuguan_topic_2", "qiuguan_tag", body);
            SendResult send = producer.send(message, 10000);
            System.out.println("sendResult: " + send);
        }

        producer.shutdown();
    }
}
