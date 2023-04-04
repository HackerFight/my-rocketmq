package com.qiuguan.rocketmq.general;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * @author qiuguan
 * @version AsyncProducer.java, v 0.1 2022/03/25  18:03:24 qiuguan Exp $
 * 异步发送消息
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
public class AsyncProducer {

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("qiuguan-p-group");
        // 设置NameServer的地址
        producer.setNamesrvAddr("127.0.0.1:9876");

        // 启动Producer实例
        producer.start();
        //循环发送5条消息
        for (int i = 0; i < 5; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("qp-topic",
                    "*",
                    ("Hello RocketMQ, producer is qiuguan " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            msg.setKeys("key-unique-" + i);

            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("sendResult : " + sendResult);
                }

                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                }
            });
        }

        // sleep一会儿，确保接受到异步的回调
        TimeUnit.SECONDS.sleep(30);

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
