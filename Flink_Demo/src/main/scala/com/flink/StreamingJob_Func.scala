package com.flink

import com.flink.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
object StreamingJob_Func {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val readings: DataStreamSource[SensorReading] = env.addSource(new SensorSource)

    val keyedDS: datastream.KeyedStream[SensorReading, String] = readings.keyBy(sensor=>sensor.id)





    keyedDS.process(new TempIncreaseAlertFunction)

    env.execute("Flink Streaming Scala API Skeleton")
  }
  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

    // 保存上一个传感器的温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
    )

    // 保存注册的定时器的时间戳
    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )


    override def processElement(
                                 value: SensorReading,
                                 ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                 out: Collector[String]
                               ): Unit = {

      // 获取上一次的温度
      val prevTemp: Double = lastTemp.value()

      // 将当前温度跟新到上一次的温度这个变量中
      lastTemp.update(value.timestamp)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0.0 || value.timestamp < prevTemp) {
        // 如果温度下降 或者是第一个 温度值; 删除定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        // 清空状态变量
        currentTimer.clear()
      } else if (value.timestamp > prevTemp && curTimerTimestamp == 0) {
        // 温度上升 并且 我们没有设置定时器

        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        // 保存这两个定时器
        currentTimer.update(timerTs)
      }
    }


    override def onTimer(
                          timestamp: Long,
                          ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                          out: Collector[String]): Unit = {
      out.collect("传感器id"+ctx.getCurrentKey+"的温度连续上升1s")
      currentTimer.clear()
    }
  }

}
