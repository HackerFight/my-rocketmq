package com.qiuguan.rocketmq.general;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author qiuguan
 * @version SyncProducer.java, v 0.1 2022/03/25  18:03:24 qiuguan Exp $
 * 同步发送消息
 *
 * <p>
 *     public enum SendStatus {
 *         SEND_OK,
 *
 *         // 刷盘超时。当Broker设置的刷盘策略为同步刷盘时才可能出 现这种异常状态。异步刷盘不会出现
 *         FLUSH_DISK_TIMEOUT,
 *
 *         // Slave同步超时。当Broker集群设置的Master-Slave的复 制方式为同步复制时才可能出现这种异常状态。异步复制不会出现
 *         FLUSH_SLAVE_TIMEOUT,
 *
 *         // 没有可用的Slave。当Broker集群设置为Master-Slave的 复制方式为同步复制时才可能出现这种异常状态。异步复制不会出现
 *         SLAVE_NOT_AVAILABLE,
 *    }
 *
 * </p>
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {

        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);
        // 设置NameServer的地址
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        // 设置当发送失败时重试发送的次数，默认为2次
        producer.setRetryTimesWhenSendFailed(3);
        // 设置发送超时时限为5s，默认3s
        producer.setSendMsgTimeout(5000);


        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(MQConstant.GENERAL_SYNC_TOPIC,
                    "*",
                    ("Hello RocketMQ, producer is qiuguan " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            // 为消息指定key
            int keyRandom = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE - 1);
            msg.setKeys("key-unique-" + keyRandom);
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%s%n", sendResult, keyRandom);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
