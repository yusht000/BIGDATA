package com.flink

import com.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object StreamingJob_DataStream {

  def main(args: Array[String]): Unit = {

    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置流的时间为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度1 如果不设置,那么默认当前机器的cpu的核数
    env.setParallelism(1)
    // 数据源的收集
    val sourceDS: DataStream[SensorReading] = env.fromCollection(
      List(SensorReading("sensor_1", 1547718199, 35.999000991),
        SensorReading("sensor_2", 1547718199, 29.199000991),
        SensorReading("sensor_3", 1547718199, 15.949000991),
        SensorReading("sensor_4", 1547718199, 25.999000991)
      ))
    sourceDS.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }
}


