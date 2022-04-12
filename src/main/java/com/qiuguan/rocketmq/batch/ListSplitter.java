package com.qiuguan.rocketmq.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * 官方提供的消息分割器
 *  消息列表分割器：其只会处理每条消息的大小不超4M的情况。
 *  若存在某条消息，其本身大小大于4M，这个分割器无法处理，其直接将这条消息构成一个子列表返回。并没有再进行分割
 *
 *  https://github.com/apache/rocketmq/blob/master/docs/cn/RocketMQ_Example.md
 */
public class ListSplitter implements Iterator<List<Message>> {

    //单条消息的最大值也是4M
    private final int SIZE_LIMIT = 1024 * 1024 * 4;

    private final List<Message> messages;

    private int currIndex;

    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int startIndex = getStartIndex();
        int nextIndex = startIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = calcMessageSize(message);
            if (tmpSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                totalSize += tmpSize;
            }
        }
        List<Message> subList = messages.subList(startIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }

    private int getStartIndex() {
        Message currMessage = messages.get(currIndex);
        int tmpSize = calcMessageSize(currMessage);
        while (tmpSize > SIZE_LIMIT) {
            currIndex += 1;
            Message message = messages.get(currIndex);
            tmpSize = calcMessageSize(message);
        }
        return currIndex;
    }

    private int calcMessageSize(Message message) {
        int tmpSize = message.getTopic().length() + message.getBody().length;
        Map<String, String> properties = message.getProperties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            tmpSize += entry.getKey().length() + entry.getValue().length();
        }
        // 增加⽇日志的开销20字节
        tmpSize = tmpSize + 20;
        return tmpSize;
    }
}