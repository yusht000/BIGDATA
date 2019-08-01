package com.flink.process.AppFuncOnWindow

import com.flink.bean
import com.flink.bean.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindow {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val minMaxTempDS: DataStream[MinMaxTemp] = readings.keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .process(new HighAndLowTempProcessFunction)

    minMaxTempDS.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }


  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class HighAndLowTempProcessFunction extends ProcessWindowFunction[
    SensorReading,
    MinMaxTemp,
    String,
    TimeWindow
    ] {
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[SensorReading],
                          out: Collector[MinMaxTemp]
                        ): Unit = {

      val iterable: Iterable[Double] = elements.map(_.temperature)
      val temps = iterable

      val end: Long = context.window.getEnd
      val windowEnd = end

      out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))
    }
  }

}

