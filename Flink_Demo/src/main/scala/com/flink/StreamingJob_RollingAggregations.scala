package com.flink

import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object StreamingJob_RollingAggregations {


  def main(args: Array[String]): Unit = {

    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

/*
    val inputDS: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2),
      (2, 3, 1),
      (2, 2, 4),
      (1, 5, 3)
    )
*/

    //val resultDS: DataStream[(Int, Int, Int)] = inputDS.keyBy(0).sum(1)

    val inputDS: DataStream[(String, List[String])] = env.fromElements(
      ("en", List("tea")),
      ("fr", List("vin")),
      ("en", List("cake"))
    )

    val resultDS: DataStream[(String, List[String])] = inputDS.keyBy(0).reduce((x, y)=>(x._1,x._2:::y._2))

    resultDS.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

}
