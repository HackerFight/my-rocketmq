package com.qiuguan.rocketmq.delay;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class Producer {

   public static void main(String[] args) throws Exception {
      // 实例化一个生产者来产生延时消息
      DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);

      producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

      // 启动生产者
      producer.start();


      for (int i = 0; i < 10; i++) {
          Message message = new Message(MQConstant.DELAY_TOPIC, ("Hello scheduled message " + i).getBytes());

          /**
           * MessageStoreConfig
           * messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
           *
           * 共18个等级，依次是从1-18
           * 比如，level=3, 表示延迟10s 消费
           */
          message.setDelayTimeLevel(10);

          // 发送消息
          producer.send(message);
      }
       // 关闭生产者
      producer.shutdown();
  }
}