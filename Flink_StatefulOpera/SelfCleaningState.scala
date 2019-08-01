package com.flink.process.StatefulOpera

import com.flink.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object SelfCleaningState {

  def main(args: Array[String]): Unit = {

  }


  class SelfCleaningTemperatureAlertFunction(val threshold: Double)

    extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
    private var lastTempState: ValueState[Double] = _
    private var lastTimerState: ValueState[Long] = _

    override def open(paramaters: Configuration) {

      val lastTempDesc: ValueStateDescriptor[Double] = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])

      lastTempState = getRuntimeContext.getState(lastTempDesc)

      val lastTimerDesc: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long]("lastTimer", classOf[Long])

      lastTimerState = getRuntimeContext.getState(lastTimerDesc)
    }


    override def processElement(
                                 value: SensorReading,
                                 ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
                                 out: Collector[(String, Double, Double)]
                               ): Unit = {
      val newTimer = ctx.timestamp() + (3600 * 1000)

      val curTimer = lastTimerState.value()

      ctx.timerService().deleteEventTimeTimer(curTimer)
      ctx.timerService().registerEventTimeTimer(newTimer)

      lastTimerState.update(newTimer)

      val lastTemp = lastTempState.value()

      val tempDiff = (value.temperature - lastTemp).abs

      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }

      this.lastTempState.update(value.temperature)
    }


    override def onTimer(
                          timestamp: Long,
                          ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#OnTimerContext,
                          out: Collector[(String, Double, Double)]
                        ): Unit = {
      // clear all state for key
      lastTempState.clear()
      lastTimerState.clear()
    }

  }

}
