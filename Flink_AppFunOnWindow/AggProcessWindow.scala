package com.flink.process.AppFuncOnWindow

import com.flink.bean
import com.flink.bean.SensorSource
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
object AggProcessWindow {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val minMaxTempDS: DataStream[MinMaxTemp] = readings.map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce(
        (r1: (String, Double, Double), r2: (String, Double, Double)) => {
          (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
        },
        new AssignWindowEndProcessFunction()
      )
    minMaxTempDS.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class AssignWindowEndProcessFunction extends ProcessWindowFunction[
    (String, Double, Double),
    MinMaxTemp,
    String,
    TimeWindow
    ] {
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Double, Double)],
                          out: Collector[MinMaxTemp]
                        ): Unit = {

      val minMax = elements.head
      val windowEnd = context.window.getEnd
      out.collect(MinMaxTemp(key, minMax._2, minMax._3, windowEnd))
    }
  }

}
