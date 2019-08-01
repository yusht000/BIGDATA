package com.flink.process.AppFuncOnWindow

import com.flink.bean
import com.flink.bean.SensorSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ReduceFunctionWindow {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val minTempDS: DataStream[(String, Double)] = readings
      .map(sensor => (sensor.id, sensor.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    minTempDS.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }
}
