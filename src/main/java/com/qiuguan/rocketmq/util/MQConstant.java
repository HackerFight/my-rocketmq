package com.qiuguan.rocketmq.util;

/**
 * @author: qiuguan
 * date: 2022/3/30 - 下午11:23
 */
public abstract class MQConstant {

    public static final String NAME_SERVER_ADDR = "47.96.111.179:9876";

    public static final String NAME_SERVER_ADDR_LOCAL = "127.0.0.1:9876";

    public static final String DEFAULT_PRODUCER_GROUP_NAME = "zhangxinyu-group";

    public static final String GENERAL_SYNC_TOPIC = "general_sync_topic";

    public static final String GENERAL_ASYNC_TOPIC = "general_async_topic";

    public static final String GENERAL_ONE_WAY_TOPIC = "general_one_way_topic";

    public static final String ORDERED_TOPIC = "ordered_topic";

    public static final String DELAY_TOPIC = "delay_test_20220409_topic";

    public static final String BATCH_TOPIC = "batch_topic";

    public static final String FILTER_TAG_TOPIC = "filter_by_tag_topic";

    public static final String FILTER_SQL_TOPIC = "filter_by_sql_test_topic";

    public static final String RETRY_TOPIC = "retry_topic";

    public static final String TX_TOPIC = "tx_topic_test";

    public static final String DEBUG_TEST_TOPIC = "offset-init-broker-topic";
}
