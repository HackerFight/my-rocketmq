package com.qiuguan.rocketmq.biz;

import lombok.Data;

/**
 * @author qiuguan
 * @date 2023/03/29 22:34:04  星期三
 */
@Data
public class Order {

    private String orderId;

    private String buyer;

    private OrderDetail orderDetail;

    private String extendInfo;
}
