package com.qiuguan.rocketmq.delay;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;

import static com.qiuguan.rocketmq.util.MQConstant.DELAY_TOPIC;

/**
 * 先启动消费者
 */
public class Consumer {

   public static void main(String[] args) throws Exception {

       System.out.println("6/20 = " + (6 / 20));

      // 实例化消费者
      DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ExampleConsumer");
      consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

      // 订阅Topics
      consumer.subscribe(DELAY_TOPIC, "*");

      // 注册消息监听者
      consumer.registerMessageListener(new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
              for (MessageExt message : messages) {
                  // Print approximate delay time period
                  System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getBornTimestamp()) + "ms later");
              }
              return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
      });
      // 启动消费者
      consumer.start();
  }
}
