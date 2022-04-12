package com.qiuguan.rocketmq.batch;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * @author: qiuguan
 * date: 2022/4/12 - 下午9:01
 */
public class Consumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        consumer.subscribe(MQConstant.BATCH_TOPIC, "someTag");

        /**
         * 指定每次可以消费10条消息，默认为 1
         */
        consumer.setConsumeMessageBatchMaxSize(10);

        /**
         * 指定每次可以从Broker拉取40条消息，默认为32
         */
        consumer.setPullBatchSize(40);

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 第一个参数虽然是一个集合，但是默认情况下只有一条数据
             * consumer.setConsumeMessageBatchMaxSize(10); 这样指定后，msgs 集合中的数据量就是10
             * @param msgs
             * @param context
             * @return
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                System.out.println("msgs.size() = " + msgs.size());
                System.out.println("----------------------------------------");

                for (MessageExt msg : msgs) {
                    System.out.println(msg);
                }

                //消费成功时返回
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

                //消费失败时返回
                //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });

        consumer.start();

        System.out.println("Batch Consumer Started");
    }
}
