package com.qiuguan.rocketmq.tx;

import com.qiuguan.rocketmq.util.MQConstant;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @author qiuguan
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionListener transactionListener = new LocalTransactionCheckListener();

        TransactionMQProducer producer = new TransactionMQProducer(MQConstant.DEFAULT_PRODUCER_GROUP_NAME);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        producer.setNamesrvAddr(MQConstant.NAME_SERVER_ADDR);
        producer.setExecutorService(executorService);
        //指定本地事务监听器
        producer.setTransactionListener(transactionListener);

        producer.start();

        String[] tags = new String[]{"tagA", "tagB", "tagC"};

        for (int i = 0; i < 3; i++) {
            try {
                Message msg =
                        new Message(MQConstant.TX_TOPIC, tags[i], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                //第二个参数用于指定在执行本地事务时要使用的业务参数
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("发送事务消息结果%s%n", sendResult);

            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        //先不要停掉生产者，观察事务回调和回查
        Thread.sleep(300000);

        producer.shutdown();
    }
}
