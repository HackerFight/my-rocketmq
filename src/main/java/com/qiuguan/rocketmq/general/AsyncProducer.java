package com.qiuguan.rocketmq.general;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.ThreadLocalRandom;
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
        DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);
        // 设置NameServer的地址
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        // 设置当发送失败时不进行重试发送
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 设置新创建的topic的queue的数量，默认是4个
        producer.setDefaultTopicQueueNums(2);


        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message(MQConstant.GENERAL_ASYNC_TOPIC,
                    "*",
                    ("Hello RocketMQ, producer is qiuguan " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            // 为消息指定key
            int keyRandom = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE - 1);
            msg.setKeys("key-unique-" + keyRandom);

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

        // sleep一会儿，由于采用的是异步发送，所以若这里不sleep，
        // 则消息还未发送就会将producer给关闭，报错
        TimeUnit.SECONDS.sleep(30);

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
