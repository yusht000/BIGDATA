package com.flink.process.function

import java.util.Calendar

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.util.Random


object StreamingJob_Fun {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val readings = env.addSource(new SensorSource).keyBy(_.id).process(new TemperatureAlert)
    readings.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }


  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  class SensorSource extends RichParallelSourceFunction[SensorReading] {

    var running: Boolean = true

    override def run(srcCtx: SourceContext[SensorReading]): Unit = {

      val rand = new Random()

      var curFTemp = (1 to 10).map {
        // nextGaussian产生高斯随机数
        i => ("sensor_" + i, 65 + (rand.nextGaussian() * 20))
      }

      while (running) {

        curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))

        val curTime = Calendar.getInstance.getTimeInMillis

        // emit new SensorReading
        // 发射新的传感器数据, 注意这里srcCtx.collect
        curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))
        // wait for 100 ms
        Thread.sleep(100)

      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  class TemperatureAlert extends KeyedProcessFunction[String, SensorReading, String] {

    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long]))

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String,
                                  SensorReading, String]#Context,
                                out: Collector[String]
                               ): Unit = {

      val prevTemp = lastTemp.value()
      // update last temperature
      // 将当前温度更新到上一次的温度这个变量中
      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()

      if (prevTemp == 0.0 || r.temperature < prevTemp) {
        // temperature decreased; delete current timer
        // 温度下降或者是第一个温度值，删除定时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp) // 清空状态变量
        currentTimer.clear()

      } else if (r.temperature > prevTemp && curTimerTimestamp == 0) {

        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(timerTs)

        // remember current timer
        currentTimer.update(timerTs)
      }
    }

    override def onTimer(
                          ts: Long,
                          ctx: KeyedProcessFunction[String,
                            SensorReading, String]#OnTimerContext,
                          out: Collector[String]): Unit = {
      out.collect("传感器报错了！")
      currentTimer.clear()
    }
  }

}
