package com.fei

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.util.HoodiePipeline

/**
 * @Description:
 * @ClassName: FlinkHudiReadWrite
 * @Author chengfei
 * @DateTime 2023/4/28 15:46
 * */
object FlinkHudiReadWrite {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    env.enableCheckpointing(2000)
    env.setParallelism(1)

    //4.读取Kakfa 中的数据
    tableEnv.executeSql(
      """
        | create table kafkaInputTable(
        |  id varchar,
        |  name varchar,
        |  age int,
        |  ts varchar,
        |  loc varchar
        | ) with (
        |  'connector' = 'kafka',
        |  'topic' = 'test_tp',
        |  'properties.bootstrap.servers'='10.191.80.158:8092',
        |  'scan.startup.mode'='earliest-offset',
        |  'properties.group.id' = 'testgroup',
        |  'format' = 'csv'
        | )
      """.stripMargin)
    val table: Table = tableEnv.from("kafkaInputTable")

    HoodiePipeline.builder("t1")
      .column("id VARCHAR(20)")
      .column("name VARCHAR(10)")
      .column("age INT")
      .column("ts VARCHAR(20)")
      .column("loc VARCHAR(20)")
      .partition("loc")
      .pk("id")
      .option(FlinkOptions.PATH,"/flink_hudi_data")
      .option(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name)
      .option(FlinkOptions.PATH,"/flink_hudi_data")
      .option(FlinkOptions.PATH,"/flink_hudi_data")

  }
}
