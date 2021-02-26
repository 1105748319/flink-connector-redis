package org.apache.flink.connector.redis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class test12 {

    public static final String REDIS_TABLE_SINK_DDL = "" +
            "CREATE TABLE redis_dim (\n" +
            "key String,\n" +
            "res String\n" +
            ") WITH (\n" +
            "  'connector.type' = 'redis',  \n" +
            "  'redis.ip' = '10.100.1.15',  \n" +
            "  'database.num' = '0', \n"+//不起作用，不知道是不是只有一个数据库的原因
            "  'operate.tpye' = 'hash', \n" +
            "  'redis.password' = 'hl.Data2018', \n" +
            "  'redis.version' = '2.6' \n" +
            ")";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);

        DataStream<Tuple2<String,String>> socketDataStream = env.socketTextStream("localhost",9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String,String> map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new Tuple2<>(data[0],data[1]);
                    }
                });

        //注册redis维表
        tEnv.sqlUpdate(REDIS_TABLE_SINK_DDL);

        //source注册成表
        tEnv.createTemporaryView("data_source", socketDataStream, "id,city,p.proctime");

        Table table1 = tEnv.sqlQuery("select * from data_source");
        tEnv.toAppendStream(table1,Row.class).print();
        //join语句
        Table table = tEnv.sqlQuery("select a.id ,b.key  from data_source a \n "+
                "left join redis_dim FOR SYSTEM_TIME AS OF a.p AS b on a.id = b.key");
        tEnv.toRetractStream(table,Row.class).print();

        //执行写redis
       // tEnv.createTemporaryView("toredis",table);
       // tEnv.executeSql("insert into redis_sink select c1,c2,c3 from toredis");

        env.execute("hashRedis");

    }
}
