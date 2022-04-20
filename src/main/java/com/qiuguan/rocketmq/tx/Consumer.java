package com.qiuguan.rocketmq.tx;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * tagA --> COMMIT_MESSAGE, 会被消费
 * tagB --> ROLLBACK_MESSAGE, 回滚了，不会消费
 * tagC --> 回查后COMMIT_MESSAGE, 也会被消费
 * 所以可以消费2条消息
 */
public class Consumer {

	public static void main(String[] args) throws InterruptedException, MQClientException {
        // 定义一个push 消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("qiuguan_group");

    	// 设置NameServer的地址
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        consumer.subscribe(MQConstant.TX_TOPIC, "tagA || tagB || tagC");
    	// 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
	}
}