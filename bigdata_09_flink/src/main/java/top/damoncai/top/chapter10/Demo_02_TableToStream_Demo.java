package top.damoncai.top.chapter10;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import top.damoncai.top.bean.Event;
import top.damoncai.top.chapter02.Demo_03_Source_Customer;

import java.time.Duration;

/**
 * <p>
 *
 * </p>
 *
 * @author zhishun.cai
 * @since 2022/3/22 14:09
 */
public class Demo_02_TableToStream_Demo {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode() // 使用流处理模式
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String creatDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                " ) WITH ( " +
                " 'connector' = 'filesystem', " +
                " 'path' = 'bigdata_09_flink/datas/clicks.txt', " +
                " 'format' = 'csv' " +
                ")";
        tableEnv.executeSql(creatDDL);

        Table tableRes = tableEnv.sqlQuery("SELECT user_name,count(url) cnt FROM clickTable GROUP BY user_name");

        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " cnt BIGINT " +
                " ) WITH ( " +
                " 'connector' = 'print' " +
                ")";

        tableEnv.executeSql(createPrintOutDDL);
        tableRes.executeInsert("printOutTable");
    }
}
