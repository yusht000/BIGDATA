package com.flink.process.CoProcessFunction

import com.flink.bean
import com.flink.bean.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StreamingJob_CoProcess {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

   // readings.keyBy(_.id).print()

    val filterSwitches: DataStream[(String, Long)] = env.fromCollection(
      List(
          ("sensor_2", 10 * 1000L),
          ("sensor_7", 10 * 1000L)
      )
    )

   val forwardedReadings: DataStream[(String, Boolean)] = readings.connect(filterSwitches).keyBy(_.id,_._1).process(new ReadingFilter)

    forwardedReadings.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }


  // 状态和流的ID 进行绑定
  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), (String,Boolean)] {

    // 传输数据的开关
    lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

    // 定时器 是否启动
    lazy val disableTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    override def processElement1(
                                  value: SensorReading,
                                  ctx: CoProcessFunction[SensorReading, (String, Long),
                                    (String,Boolean)]#Context,
                                  out: Collector[(String,Boolean)]
                                ): Unit = {

      if (forwardingEnabled.value()) {

        out.collect((value.id,forwardingEnabled.value()))
      }else{
        out.collect((value.id,forwardingEnabled.value()))
      }

    }

    // 数据是一条一条的传播
    override def processElement2(
                                  value: (String, Long),
                                  ctx: CoProcessFunction[SensorReading, (String, Long), (String, Boolean)]#Context,
                                  out: Collector[(String,Boolean)]
                                ): Unit = {

      // 允许继续传输数据
      forwardingEnabled.update(true)

      // set disable forward timer
      val timerTimestamp: Long = ctx.timerService().currentProcessingTime() + value._2

      val curTimerTimestamp: Long = disableTimer.value()

      if (timerTimestamp > curTimerTimestamp) {
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)

        ctx.timerService().registerProcessingTimeTimer(timerTimestamp)

        disableTimer.update(timerTimestamp)
      }

      out.collect((value._1+"processElement_2: ",forwardingEnabled.value()))

    }

    override def onTimer(
                          timestamp: Long,
                          ctx: CoProcessFunction[SensorReading, (String, Long),(String,Boolean)]#OnTimerContext,
                          out: Collector[(String,Boolean)]
                        ): Unit = {

      forwardingEnabled.clear()

      disableTimer.clear()
    }
  }
}
