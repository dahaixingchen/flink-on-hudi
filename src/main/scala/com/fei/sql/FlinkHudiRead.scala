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
object FlinkHudiRead {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    System.setProperty("HADOOP_USER_NAME", "hdfs")

    env.enableCheckpointing(2000)
    env.setParallelism(1)

    //4.读取Kakfa 中的数据
    tableEnv.executeSql(
      """
        | create table t1(
        |  uuid varchar,
        |  name varchar,
        |  age int,
        |  dt varchar,
        |  pt varchar
        | ) with (
        |  'connector' = 'hudi',
        |  'path' = '/user/chengfei/hudi-t2',
        |  'write.tasks' = '1', -- default is 4 ,required more resource
        |  'compaction.tasks' = '1', -- default is 10 ,required more resource
        |  'table.type' = 'COPY_ON_WRITE' -- this creates a MERGE_ON_READ table, by default is COPY_ON_WRITE
        | )
      """.stripMargin)

    //把数据写入hidi中
    tableEnv.executeSql(
      s"""
         | select * from t1
    """.stripMargin
    ).print()


  }
}
