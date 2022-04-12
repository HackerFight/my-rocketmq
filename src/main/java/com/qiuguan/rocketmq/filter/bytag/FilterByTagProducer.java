package com.qiuguan.rocketmq.filter.bytag;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @author: qiuguan
 * date: 2022/4/12 - 下午10:26
 */
public class FilterByTagProducer {

    public static void main(String[] args) throws Exception {

        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);
        // 设置NameServer的地址
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        producer.start();

        String[] tags = {"filter_tag_a", "filter_tag_b", "filter_tag_c"};

        for (int i = 0; i < 10 ; i++) {

            byte[] body = ("Hi filter message," + i).getBytes();
            String tag = tags[i % tags.length];

            //同一个topic下，会发送多种tag消息
            Message msg = new Message(MQConstant.FILTER_TAG_TOPIC, tag, body);

            SendResult sendResult = producer.send(msg);

            System.out.println("sendResult = " + sendResult);
        }


        producer.shutdown();
    }
}
