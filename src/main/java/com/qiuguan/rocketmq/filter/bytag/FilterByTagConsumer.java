package com.qiuguan.rocketmq.filter.bytag;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author: qiuguan
 * date: 2022/4/12 - 下午10:32
 */
public class FilterByTagConsumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        /**
         * 订阅消息过滤
         * 我只订阅 topic = filter_by_tag_topic 下
         * tag = filter_tag_a 或者 tag = filter_tag_c 的消息
         * 不要 tag = filter_tag_b 的消息
         */
        consumer.subscribe(MQConstant.FILTER_TAG_TOPIC, "filter_tag_a || filter_tag_c");

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

        System.out.println("Filter Tag Consumer Started");
    }
}
