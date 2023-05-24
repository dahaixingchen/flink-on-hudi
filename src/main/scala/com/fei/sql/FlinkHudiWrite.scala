package com.fei.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.configuration.FlinkOptions
import org.apache.hudi.util.HoodiePipeline

import java.util.Properties

/**
 * @Description:
 * @ClassName: FlinkHudiReadWrite
 * @Author chengfei
 * @DateTime 2023/4/28 15:46
 * */
object FlinkHudiWrite {
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

    tableEnv.executeSql(
      """
        |CREATE TABLE t1(
        |  id VARCHAR(20) PRIMARY KEY NOT ENFORCED,--默认主键列为uuid,这里可以后面跟上“PRIMARY KEY NOT ENFORCED”指定为主键列
        |  name VARCHAR(10),
        |  age INT,
        |  ts VARCHAR(20),
        |  loc VARCHAR(20)
        |)
        |PARTITIONED BY (loc)
        |WITH (
        |  'connector' = 'hudi',
        |  'path' = '/flink_hudi_data',
        |  'write.tasks' = '1', -- default is 4 ,required more resource
        |  'compaction.tasks' = '1', -- default is 10 ,required more resource
        |  'table.type' = 'COPY_ON_WRITE' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
        |)
      """.stripMargin)

    //把数据写入hidi中
    tableEnv.executeSql(
      s"""
         | insert into t1 select id,name,age,ts,loc from ${table}
    """.stripMargin
    )





  }
}
