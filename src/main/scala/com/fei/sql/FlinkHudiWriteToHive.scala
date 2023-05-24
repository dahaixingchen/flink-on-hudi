package com.fei.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @Description:
 * @ClassName: FlinkHudiReadWrite
 * @Author chengfei
 * @DateTime 2023/4/28 15:46
 * */
object FlinkHudiWriteToHive {
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
        |  uuid varchar,
        |  name varchar,
        |  age int,
        |  dt BIGINT,
        |  pt varchar
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
        |CREATE TABLE flink_hudi_cow(
        |  uuid VARCHAR(20) PRIMARY KEY NOT ENFORCED,--默认主键列为uuid,这里可以后面跟上“PRIMARY KEY NOT ENFORCED”指定为主键列
        |  name VARCHAR(10),
        |  age INT,
        |  dt BIGINT,
        |  pt VARCHAR(20)
        |)
        |PARTITIONED BY (pt)
        |WITH (
        |  'connector'='hudi',
        |  'path' = '/user/chengfei/hudi-copyOnWrite', -- 创建不同的merge_on_read表和 copy_on_write表的时候需要写入不同的路径，创建不同的表就应该创建不同的路径
        |  'table.type'='COPY_ON_WRITE',        -- MERGE_ON_READ 方式在没生成 parquet 文件前，hive不会有输出.COPY_ON_WRITE 方式只生成一张表
        |  'hoodie.cleaner.commits.retained'='2',
        |  'hive_sync.enable'='true',           -- required，开启hive同步功能
        |  'hive_sync.metastore.uris' = 'thrift://cdh-7253:9083', -- required, metastore的端口
        |  'hive_sync.jdbc_url'='jdbc:hive2://cdh-7253:10000',    -- required, hiveServer地址
        |  'hive_sync.table'='flink_hudi_cow',                          -- required, hive 新建的表名
        |  'hive_sync.db'='hudi_db',                         -- required, hive 新建的数据库名
        |  'hive_sync.username'='ykas_aq',                     -- required, HMS 用户名
        |  'hive_sync.password'='XwHdDvzwLRrdKvM3' ,
        |  'read.streaming.enabled' = 'true',
        |  'read.streaming.start-commit' = '20210901151206' ,
        |  'read.streaming.check-interval' = '4' ,
        |  'write.tasks'='1'
        |)
      """.stripMargin)

    //把数据写入hidi中
    tableEnv.executeSql(
      s"""
         | insert into flink_hudi_cow select uuid,name,age,dt,pt from ${table}
    """.stripMargin
    )


  }
}
