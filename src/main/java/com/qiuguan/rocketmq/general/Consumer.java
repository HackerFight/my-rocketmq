package com.qiuguan.rocketmq.general;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

public class Consumer {

	public static void main(String[] args) throws InterruptedException, MQClientException {

	    // 定义一个 pull 消费者
        //DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("qiuguan_group");

        // 定义一个push 消费者
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("hold5-c-group");

    	// 设置NameServer的地址
        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR_LOCAL);


        //指定从第一条消息开始消费
        //consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //设置消费模式来广播模式，默认是集群模式
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        //consumer.setConsumeMessageBatchMaxSize(32);
        //consumer.setPullInterval(100);

    	// 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        //MQConstant.GENERAL_SYNC_TOPIC
        //MQConstant.GENERAL_ONE_WAY_TOPIC
        //MQConstant.GENERAL_ASYNC_TOPIC
        consumer.subscribe(MQConstant.DEBUG_TEST_TOPIC, "*");
    	// 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 标记该消息已经被成功消费
                context.setAckIndex(msgs.size());
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
	}
}