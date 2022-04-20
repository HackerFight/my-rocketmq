package com.qiuguan.rocketmq.tx;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;


/**
 * 本地事务监听器
 *
 * @author qiuguan
 */
public class LocalTransactionCheckListener implements TransactionListener {

    /**
     * @param msg
     * @param arg 他就是 producer.sendMessageInTransaction(msg, null); 的第二个参数null
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("预提交消息成功：" + msg);

        /**
         * 假设接收到tagA的消息就表示扣款操作成功，
         * tagB的消息表示扣款失败，
         * tagC表示扣款结果不清楚，需要执行消息回查
         */
        if (StringUtils.equals("tagA", msg.getTags())) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (StringUtils.equals("tagB", msg.getTags())) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else if (StringUtils.equals("tagC", msg.getTags())) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.UNKNOW;
    }


    /**
     * tagC 的本地事务状态是 UNKNOW, 所以会引起回查
     * 回查后是 COMMIT_MESSAGE, 那么则最终会提交该消息，被消费者看到消费
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println("topic = " + msg.getTopic() + ", tag = " + msg.getTags() + " 消息回查");
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
