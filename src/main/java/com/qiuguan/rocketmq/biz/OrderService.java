package com.qiuguan.rocketmq.biz;

import com.sun.org.apache.xpath.internal.operations.Or;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * @author qiuguan
 * @date 2023/03/29 22:39:26  星期三
 */
public class OrderService {

    private OrderMapper orderMapper;

    private OrderDetailMapper orderDetailMapper;

    private final DefaultMQProducer producer = new DefaultMQProducer("test");

    private TransactionTemplate transactionTemplate;

    /**
     * 先写库后发消息
     * @param order
     * @throws Exception
     */
    public void createOrder(final Order order) throws Exception {

        transactionTemplate.execute(new TransactionCallback<Boolean>() {
            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                orderMapper.save(order);
                orderDetailMapper.save(order.getOrderDetail());

                Message orderMessage = new Message();
                orderMessage.setBody(order.toString().getBytes());
                SendResult send = null;
                try {
                    send = producer.send(orderMessage);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                assert send != null;
                if (send.getSendStatus() == SendStatus.SEND_OK) {
                    status.setRollbackOnly();
                }

                return Boolean.TRUE;
            }
        });

    }

    public void saveOrder2(Order order) throws Exception {

        SendResult send = producer.send((Message) null);
        if (send.getSendStatus() == SendStatus.SEND_OK) {
            orderMapper.save(order);
            orderDetailMapper.save(order.getOrderDetail());
        }
    }
}
