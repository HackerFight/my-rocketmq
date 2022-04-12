package com.qiuguan.rocketmq.batch;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: qiuguan
 * date: 2022/4/10 - 下午11:39
 */
public class Producer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);
        // 设置NameServer的地址
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        // 指定要发送的消息的最大大小，默认是4M
        // 不过，仅修改该属性是不行的，还需要同时修改broker加载的配置文件中的
        // 在服务端的 MessageStoreConfig#maxMessageSize
        //他们要配合一起工作，单改一个没用
        producer.setMaxMessageSize(4 * 1024 * 1024);

        producer.start();

        // 定义要发送的消息集合
        List<Message> messages = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message(MQConstant.BATCH_TOPIC, "someTag", body);
            messages.add(msg);
        }


        // 定义消息列表分割器，将消息列表分割为多个不超出4M大小的小列表
        //参考官网：https://github.com/apache/rocketmq/blob/master/docs/cn/RocketMQ_Example.md
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()) {
            List<Message> subMessage = splitter.next();
            producer.send(subMessage);
        }

        producer.shutdown();
    }
}
