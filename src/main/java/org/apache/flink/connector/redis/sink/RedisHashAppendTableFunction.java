package org.apache.flink.connector.redis.sink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.util.MyRedisCommand;
import org.apache.flink.connector.redis.util.MyRedisCommandsContainer;
import org.apache.flink.connector.redis.util.MyRedisCommandsContainerBuilder;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
public class RedisHashAppendTableFunction extends RichSinkFunction<Row>  {//implements CheckpointedFunction
    private static final Logger LOG = LoggerFactory.getLogger(RedisHashAppendTableFunction.class);

    private final String connectIp;
    private final String connectPassword;
    private final String operateType;
    private MyRedisCommand redisCommand;
    private FlinkJedisConfigBase flinkJedisConfigBase;
    private MyRedisCommandsContainer redisCommandsContainer;


    public RedisHashAppendTableFunction(String connectIp,String connectPassword, String operateType) {
        this.connectIp = connectIp;
        this.connectPassword = connectPassword;
        this.operateType = operateType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {

            this.flinkJedisConfigBase = new FlinkJedisPoolConfig.Builder().setHost(connectIp).setPort(6379).setPassword(connectPassword).build();
            this.redisCommand = MyRedisCommand.HGET;
            this.redisCommandsContainer = MyRedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);

        } catch (Exception e) {
            throw new Exception("建立redis异步客户端失败", e);
        }
//        super.open(parameters);
    }

    @Override
    public void invoke(Row value, Context context) throws Exception {
        try {
           // TODO 这里做sink相关操作

        }catch (Exception e){
            LOG.error("写redis失败", e);
            throw new RuntimeException("写redis失败", e);
        }

        LOG.info("sink结果:{}",value.toString());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.redisCommandsContainer != null) {
            this.redisCommandsContainer.close();
        }
    }


}
