package org.apache.flink.connector.redis.util;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;

public enum MyRedisCommand {

    HGET(RedisDataType.STRING);

    private RedisDataType redisDataType;

    private MyRedisCommand(RedisDataType redisDataType) {
        this.redisDataType = redisDataType;
    }

    public RedisDataType getRedisDataType() {
        return this.redisDataType;
    }
}

