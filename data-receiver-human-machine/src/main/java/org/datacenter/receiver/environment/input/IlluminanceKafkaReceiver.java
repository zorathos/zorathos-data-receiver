package org.datacenter.receiver.environment.input;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.datacenter.config.receiver.environment.EnvironmentKafkaReceiverConfig;
import org.datacenter.model.base.TiDBDatabase;
import org.datacenter.model.environment.input.Illuminance;
import org.datacenter.receiver.environment.base.EnvironmentKafkaReceiver;
import org.datacenter.receiver.util.JdbcSinkUtil;

/**
 * @author : [wangminan]
 * @description : [一句话描述该类的功能]
 */
public class IlluminanceKafkaReceiver extends EnvironmentKafkaReceiver<Illuminance> {

    public static void main(String[] args) {
        EnvironmentKafkaReceiverConfig config = parseArgs(args);
        IlluminanceKafkaReceiver receiver = new IlluminanceKafkaReceiver();
        receiver.setConfig(config);
        receiver.run();
    }

    @Override
    public void prepare() {
        super.prepare();
        modelClass = Illuminance.class;
    }

    @Override
    protected Sink<Illuminance> createSink(Long importId) {
        return JdbcSink.<Illuminance>builder()
                .withQueryStatement("""
                        INSERT INTO illuminance (
                            task_id, device_id, timestamp, import_id,
                            illuminance
                        ) VALUES (
                            ?, ?, ?, ?, ?,
                            ?
                        );
                        """, (preparedStatement, illuminance) -> {
                    preparedStatement.setLong(1, illuminance.getTaskId());
                    preparedStatement.setLong(2, illuminance.getDeviceId());
                    preparedStatement.setTimestamp(3, illuminance.getTimestamp() == null ?
                            null : java.sql.Timestamp.valueOf(illuminance.getTimestamp()));
                    preparedStatement.setLong(4, importId);
                    preparedStatement.setObject(5, illuminance.getIlluminance());
                })
                .withExecutionOptions(JdbcSinkUtil.getTiDBJdbcExecutionOptions())
                .buildAtLeastOnce(JdbcSinkUtil.getTiDBJdbcConnectionOptions(TiDBDatabase.ENVIRONMENT));
    }

    @Override
    protected String getReceiverName() {
        return "IlluminanceKafkaReceiver";
    }
}
