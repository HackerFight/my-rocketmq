package com.qiuguan.rocketmq.retry;

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
 *
 * 参数参考阿里云：https://help.aliyun.com/document_detail/150027.html
 */
public class Consumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        consumer.subscribe(MQConstant.RETRY_TOPIC, "someTag");

        /**
         * '顺序消息'最小重试间隔，默认值：1000，单位：毫秒，允许区间为[10,30000]。
         *  它是通过时间来控制的
         */
        consumer.setSuspendCurrentQueueTimeMillis(1000);

        /**
         * 最大重试次数，对于非顺序消息生效
         */
        consumer.setMaxReconsumeTimes(20);

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

                try {
                    for (MessageExt msg : msgs) {
                        System.out.println(msg);
                    }

                    //消费成功时返回，不会重试
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    e.printStackTrace();

                    //抛出异常会重试
                    throw e;

                    // 返回这个状态码也会重试，官方推荐返回这个
                    //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }


                //返回 null 会重试
                //return null;
            }
        });

        consumer.start();

        System.out.println("Batch Consumer Started");
    }
}
