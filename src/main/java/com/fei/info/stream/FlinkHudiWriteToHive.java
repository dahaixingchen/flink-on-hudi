package com.fei.info.stream;

import com.fei.info.Personas;
import com.fei.util.CustomKafkaDeserSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Description:
 * @ClassName: FlinkHudiWriteToHive
 * @Author chengfei
 * @DateTime 2023/5/23 17:40
 **/
public class FlinkHudiWriteToHive {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000 * 60);
        env.setParallelism(1);

        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.191.80.158:8092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroup");
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        DataStream<Personas> dStream = env.addSource(new FlinkKafkaConsumer(
                "test_tp"
                , new CustomKafkaDeserSchema()
                , consumerProps
        ));
        DataStream<RowData> map = dStream.map(p -> {

            GenericRowData genericRowData = new GenericRowData(5);
            genericRowData.setField(0, StringData.fromString(p.getUuid()));
            genericRowData.setField(1, StringData.fromString(p.getName()));
            genericRowData.setField(2, StringData.fromString(String.valueOf(p.getAge())));
            genericRowData.setField(3, StringData.fromString(String.valueOf(p.getDt())));
            genericRowData.setField(4, StringData.fromString(p.getPt()));
            return genericRowData;
        });
        map.print();

        HoodiePipeline.Builder builderCow = HoodiePipeline.builder("flink_hudi_stream")
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age VARCHAR(10)")
                .column("dt VARCHAR(20)")
                .column("pt VARCHAR(20)")
                .partition("pt")
                .pk("uuid")
                .option(FlinkOptions.PATH.key(), "/user/chengfei/hudi-flink-stream")
                .option(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name())
                .option(FlinkOptions.COMPACTION_TASKS.key(), 1)
                .option(FlinkOptions.WRITE_TASKS.key(), 1)
                .option(FlinkOptions.PRECOMBINE_FIELD.key(), "dt") //当主键冲突的时候以什么字段的最大值为标准
        //自动开启创建表
                .option(FlinkOptions.HIVE_SYNC_ENABLED.key(), true)
                .option(FlinkOptions.READ_AS_STREAMING.key(), true)
                .option(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), "thrift://cdh-7253:9083")
                .option(FlinkOptions.HIVE_SYNC_JDBC_URL.key(), "jdbc:hive2://cdh-7253:10000")
                .option(FlinkOptions.HIVE_SYNC_TABLE.key(), "flink_hudi_stream")
                .option(FlinkOptions.HIVE_SYNC_DB.key(), "hudi_db")
                .option(FlinkOptions.HIVE_SYNC_USERNAME.key(), "ykas_aq")
                .option(FlinkOptions.HIVE_SYNC_PASSWORD.key(), "XwHdDvzwLRrdKvM3")
                .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key(), 4);

        builderCow.sink(map, false);


        HoodiePipeline.Builder builderMor = HoodiePipeline.builder("flink_hudi_mor_stream")
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age VARCHAR(10)")
                .column("dt VARCHAR(20)")
                .column("pt VARCHAR(20)")
                .partition("pt")
                .pk("uuid")
                .option(FlinkOptions.PATH.key(), "/user/chengfei/hudi-flink-mor-stream")
//                .option(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name())
                .option(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name())
                .option(FlinkOptions.COMPACTION_TASKS.key(), 1)
                .option(FlinkOptions.WRITE_TASKS.key(), 1)
                .option("hoodie.compact.inline",true) //开启是否一个事务就开启压缩
                .option("hoodie.cleaner.commits.retained",1)  //保留多少个历史的parquet文件
                .option("hoodie.compact.inline.max.delta.commits",1) // 提交多少次合并log文件大宋新的parquet文件中
                .option(FlinkOptions.PRECOMBINE_FIELD.key(), "dt") //当主键冲突的时候以什么字段的最大值为标准
                //自动开启创建表
                .option(FlinkOptions.HIVE_SYNC_ENABLED.key(), true)
                .option(FlinkOptions.READ_AS_STREAMING.key(), true)
                .option(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key(), "thrift://cdh-7253:9083")
                .option(FlinkOptions.HIVE_SYNC_JDBC_URL.key(), "jdbc:hive2://cdh-7253:10000")
                .option(FlinkOptions.HIVE_SYNC_TABLE.key(), "flink_hudi_mor_stream")
                .option(FlinkOptions.HIVE_SYNC_DB.key(), "hudi_db")
                .option(FlinkOptions.HIVE_SYNC_USERNAME.key(), "ykas_aq")
                .option(FlinkOptions.HIVE_SYNC_PASSWORD.key(), "XwHdDvzwLRrdKvM3")
                .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key(), 4);

        builderMor.sink(map, false);

        env.execute();
    }
}
