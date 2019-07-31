package com.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
object StreamingJob_flatMap {

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val itrDS: DataStream[List[Int]] = env.fromCollection(List(List(1,2,3)))

    val listDS: DataStream[Int] = itrDS.flatMap(list=>list)

    listDS.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }

}
