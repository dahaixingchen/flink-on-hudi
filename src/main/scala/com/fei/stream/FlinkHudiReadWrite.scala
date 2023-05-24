//package com.fei.stream
//
//import com.fei.info.Personas
//import com.fei.util.CustomKafkaDeserSchema
//import org.apache.flink.formats.json.JsonRowSerializationSchema
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.table.api.{DataTypes, EnvironmentSettings}
//import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataSetConversions}
//import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
//import org.apache.hudi.common.model.HoodieTableType
//import org.apache.hudi.configuration.FlinkOptions
//import org.apache.hudi.util.HoodiePipeline
//import org.apache.kafka.clients.consumer.ConsumerConfig
//
//import java.util.Properties
//
///**
// * @Description:
// * @ClassName: FlinkHudiReadWrite
// * @Author chengfei
// * @DateTime 2023/4/28 15:46
// * */
//object FlinkHudiReadWrite {
//  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
//    System.setProperty("HADOOP_USER_NAME", "hdfs")
//
//    val consumerProps = new Properties()
//    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.191.80.158:8092")
//    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testgroup")
//    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
//
//    env.enableCheckpointing(1000*60)
//    env.setParallelism(1)
//
//    //4.读取Kakfa 中的数据
////    tableEnv.executeSql(
////      """
////        | create table kafkaInputTable(
////        |  id varchar,
////        |  name varchar,
////        |  age int,
////        |  ts varchar,
////        |  loc varchar
////        | ) with (
////        |  'connector' = 'kafka',
////        |  'topic' = 'test_tp',
////        |  'properties.bootstrap.servers'='10.191.80.158:8092',
////        |  'scan.startup.mode'='earliest-offset',
////        |  'properties.group.id' = 'testgroup',
////        |  'format' = 'csv'
////        | )
////      """.stripMargin)
////    val table: Table = tableEnv.from("kafkaInputTable")
//
//
//    val dStream: DataStream[Personas] = env.addSource(new FlinkKafkaConsumer[Personas](
//      "test_tp"
//      , new CustomKafkaDeserSchema()
//      , consumerProps
//    ))
//
//    dStream.print
//
//    val builder: HoodiePipeline.Builder = HoodiePipeline.builder("t1")
//      .column("id VARCHAR(20)")
//      .column("name VARCHAR(10)")
//      .column("age INT")
//      .column("ts VARCHAR(20)")
//      .column("loc VARCHAR(20)")
//      .partition("loc")
//      .pk("id")
//      .option(FlinkOptions.PATH, "/flink_hudi_data")
//      .option(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name)
//      .option(FlinkOptions.COMPACTION_TASKS, 1)
//      .option(FlinkOptions.WRITE_TASKS, 1)
//      .option(FlinkOptions.PRECOMBINE_FIELD.key(), 1)
//      //自动开启创建表
//      .option(FlinkOptions.HIVE_SYNC_ENABLED, true)
//      .option(FlinkOptions.HIVE_SYNC_METASTORE_URIS, "thrift://cdh-7253:9083")
//      .option(FlinkOptions.HIVE_SYNC_JDBC_URL, "jdbc:hive2://cdh-7253:10000")
//      .option(FlinkOptions.HIVE_SYNC_TABLE, "t1")
//      .option(FlinkOptions.HIVE_SYNC_DB, "hudi_db")
//      .option(FlinkOptions.HIVE_SYNC_USERNAME, "ykas_aq")
//      .option(FlinkOptions.HIVE_SYNC_PASSWORD, "XwHdDvzwLRrdKvM3")
//      .option(FlinkOptions.READ_AS_STREAMING, true)
//      .option(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 4)
//    //      .option(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE)
//
//    //        builder.sink(tableEnv.toAppendStream(table,org.apache.flink.types.Row.).,true)
//    builder.sink(dStream,true)
//    env.execute()
//  }
//}
