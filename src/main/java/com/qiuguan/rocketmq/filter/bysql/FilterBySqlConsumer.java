package com.qiuguan.rocketmq.filter.bysql;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author: qiuguan
 * date: 2022/4/12 - 下午10:32
 */
public class FilterBySqlConsumer {

    public static void main(String[] args) throws Exception {

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        consumer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        /**
         * 订阅消息过滤: 根据消息生产者指定的用户属性进行过滤
         * 支持的常量类型：
         *   数值：比如：123，3.1415
         *   字符：必须用单引号包裹起来，比如：'abc'
         *   布尔：TRUE 或 FALSE
         *   NULL：特殊的常量，表示空
         *
         * 支持的运算符有：
         *   数值比较：>，>=，<，<=，BETWEEN，=
         *   字符比较：=，<>，IN
         *   逻辑运算 ：AND，OR，NOT
         *   NULL判断：IS NULL 或者 IS NOT NULL
         *
         *   // (age between 6 and 9) AND (name IS NOT NULL) AND (isGender = TRUE)
         */
        consumer.subscribe(MQConstant.FILTER_SQL_TOPIC, MessageSelector.bySql("(age between 6 and 9) AND (name IS NOT NULL) AND (isGender = TRUE)"));

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

        System.out.println("Filter SQL Consumer Started");
    }
}
