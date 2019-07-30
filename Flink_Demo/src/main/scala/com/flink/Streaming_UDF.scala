package com.flink

import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Streaming_UDF {

  def main(args: Array[String]): Unit = {

    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val inputDS: DataStream[String] = env.fromCollection(
      List("Flink is good at realtime", "spark is not good at realtime")
    )

    //    val resultDS: DataStream[String] = inputDS.filter(new RichFilterFunction[String] {
    //      override def filter(value: String): Boolean = {
    //        ! value.contains("Flink")
    //      }
    //    })


    //inputDS.flatMap(new MyFlatMap)

    val resultDS: DataStream[String] = inputDS.filter(_.contains("Flink"))
    resultDS.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }
}


class MyFlatMap extends RichFlatMapFunction[String, (String,String)] {

  var subTaskIndex = 0

  override def open(parameters: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    // TODO 开始开流 进行数据的输入
  }

  override def flatMap(value: String, out: Collector[(String, String)]): Unit = {
     if("flink".contains(value)){
       out.collect(("flink",value))
     }
  }
  override def close(): Unit = {
    // todo  关闭数据流
  }
}




