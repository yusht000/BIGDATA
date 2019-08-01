package com.flink.process.WithState

import com.flink.bean
import com.flink.bean.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._

object MapWithState {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val keyedStream: KeyedStream[SensorReading, String] = readings.keyBy(sensor => sensor.id)

    val stateStream: DataStream[SensorReading] = keyedStream.mapWithState(

      (sensor: SensorReading, key: Option[String]) => {
        key match {
          case Some(c) => (sensor, Some(c))
          case None => (sensor, Some(sensor.id))
        }
      }
    )
    stateStream.print()
    env.execute("Flink Streaming Scala API Skeleton")

  }
}
