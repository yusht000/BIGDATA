package com.flink

import com.flink.bean.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingJob_MultiStream {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val parisStream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val tokyoStream: DataStream[SensorReading] = env.addSource(new SensorSource)
    val rioStream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val allCities: DataStream[SensorReading] = parisStream.union(tokyoStream,rioStream)

    allCities.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
