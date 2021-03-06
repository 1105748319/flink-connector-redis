package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * @ClassName: RedisLookupTableSource
 * @Description: 工厂的实现
 * @date: 2021/02/25
 */

public class RedisLookupTableSource implements LookupableTableSource<Row>, StreamTableSource<Row> {
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;

    private final String connectIp;
    private final String connectPassword;
    private final String databaseNum;
    private final String operateType;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    //获取RedisTableSourceSinkFactory传过来的参数
    private RedisLookupTableSource(String[] fieldNames, TypeInformation[] fieldTypes,
                                    String connectIp,String connectPassword, String databaseNum, String operateType,
                                   long cacheMaxSize, long cacheExpireMs) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        this.connectIp = connectIp;
        this.connectPassword = connectPassword;
        this.databaseNum = databaseNum;
        this.operateType = operateType;

        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
    }

    //返回同步的，这里不需要
    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        return null;
    }

    //返回异步的
    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        return RedisLookupFunction.builder()
                .setFieldNames(fieldNames)
                .setFieldTypes(fieldTypes)
                .setConnectIp(connectIp)
                .setConnectPassword(connectPassword)
                .setDatabaseNum(databaseNum)
                .setOperateType(operateType)
                .setCacheMaxSize(cacheMaxSize)
                .setCacheExpireMs(cacheExpireMs)
                .build();
    }

    //表示异步
    @Override
    public boolean isAsyncEnabled() {
        return true;
    }

    //获取表结构
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    //获取为数据源用，这里只做维表用，所以不需要
    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        throw new IllegalArgumentException("目前不支持作数据源用");
    }

    //数据输出结构
    @Override
    public DataType getProducedDataType() {
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        private String connectIp;
        private String connectPassword;
        private String databaseNum;
        private String operateType;

        private long cacheMaxSize;
        private long cacheExpireMs;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setConnectIp(String connectIp) {
            this.connectIp = connectIp;
            return this;
        }
        public Builder setConnectPassword(String connectPassword) {
            this.connectPassword = connectPassword;
            return this;
        }

        public Builder setDatabaseNum(String databaseNum) {
            this.databaseNum = databaseNum;
            return this;
        }

        public Builder setOperateType(String operateType) {
            this.operateType = operateType;
            return this;
        }

        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        public RedisLookupTableSource build() {
            return new RedisLookupTableSource(fieldNames, fieldTypes,
                    connectIp,connectPassword, databaseNum, operateType,
                    cacheMaxSize, cacheExpireMs);
        }
    }
}
