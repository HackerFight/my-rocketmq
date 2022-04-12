package com.qiuguan.rocketmq.filter.bysql;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import javax.management.StringValueExp;
import java.util.Random;

/**
 * @author: qiuguan
 * date: 2022/4/12 - 下午10:26
 */
public class FilterBySqlProducer {

    public static void main(String[] args) throws Exception {

        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);

        // 设置NameServer的地址
        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);

        producer.start();

        for (int i = 0; i < 10 ; i++) {

            byte[] body = ("Hi filter message," + i).getBytes();

            Message msg = new Message(MQConstant.FILTER_SQL_TOPIC, "sql_tag", body);

            /**
             * 设置用户属性
             * 注意：如果消费者根据用户属性进行过滤，那么请一定在broker的配置文件中开启:
             *  enablePropertyFilter = true
             */
            msg.putUserProperty("age", String.valueOf(i));
            msg.putUserProperty("name", "name" + (i + 1));
            msg.putUserProperty("isGender", String.valueOf(new Random().nextBoolean()));

            SendResult sendResult = producer.send(msg);

            System.out.println("sendResult = " + sendResult);
        }


        producer.shutdown();
    }
}
