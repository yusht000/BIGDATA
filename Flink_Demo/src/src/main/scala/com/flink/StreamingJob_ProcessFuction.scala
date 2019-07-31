import java.util.Calendar

import org.apache.flink.api.common.functions.{AggregateFunction, FilterFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object StreamingJob_ProcessFuction {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings = env.addSource(new SensorSource).keyBy(_.id).process(new TemperatureAlert)
    readings.print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  class LateReadingsFilter
    extends ProcessFunction[SensorReading, SensorReading] {
    val lateReadingsOut = new OutputTag[SensorReading]("late-readings")
    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      // compare record timestamp with current watermark
      if (r.timestamp < ctx.timerService().currentWatermark()) {
        // this is a late reading => redirect it to the side output ctx.output(lateReadingsOut, r)
      } else {
        out.collect(r)
      }
    }
  }

  case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)

  class HighAndLowTempProcessFunction extends ProcessWindowFunction[(String, Double, Double),
    MinMaxTemp,String,TimeWindow] {

    override def process(key: String,
                         ctx: Context,
                         in: Iterable[(String, Double, Double)],
                         out: Collector[MinMaxTemp]): Unit = {
      //      val temps = in.map(_.temperature)
      //      val windowEnd = ctx.window.getEnd
      val min = in.iterator.next._2
      val max = in.iterator.next._3
      val windowEnd = ctx.window.getEnd

      out.collect(MinMaxTemp(key, min, max, windowEnd))
    }


  }

  class AvgTempAggregate extends AggregateFunction[(String, Double),
    (String, Double, Int), (String, Double)] {
    override def createAccumulator(): (String, Double, Int) = {
      ("", 0.0, 0)
    }

    override def add(in: (String, Double), acc: (String, Double, Int)) = {
      (in._1, in._2 + acc._2, acc._3 + 1)
    }

    override def getResult(accumulator: (String, Double, Int)): (String, Double) = {
      (accumulator._1, accumulator._2 / accumulator._3)
    }

    override def merge(acc1: (String, Double, Int), acc2: (String, Double, Int)) = {
      (acc1._1, acc1._2 + acc2._2, acc1._3 + acc2._3)
    }
  }

  class ReadingFilter extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.
      getState(
        new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean]))

    lazy val disableTimer: ValueState[Long] = getRuntimeContext
      .getState(
        new ValueStateDescriptor[Long]("timer", Types.of[Long]))

    override def processElement1(reading: SensorReading,
                                 ctx: CoProcessFunction[SensorReading,
                                   (String, Long),
                                   SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      if (forwardingEnabled.value()) {
        out.collect(reading)
      }
    }

    override def processElement2(switch: (String, Long),
                                 ctx: CoProcessFunction[SensorReading,
                                   (String, Long),
                                   SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      forwardingEnabled.update(true)
      // set disable forward timer
      val timerTimestamp = ctx.timerService().currentProcessingTime() + switch
        ._2
      val curTimerTimestamp = disableTimer.value()

      if (timerTimestamp > curTimerTimestamp) {
        // remove current timer and register new timer
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
        disableTimer.update(timerTimestamp)
      }
    }

    override def onTimer(ts: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long),
                           SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      forwardingEnabled.clear()
      disableTimer.clear()
    }
  }

  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
    lazy val freezingAlarmOutput: OutputTag[String] =
      new OutputTag[String]("freezing-alarms")

    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      if (r.temperature < 100.0) {
        ctx.output(freezingAlarmOutput, s"${r.id}报警了！")
      }

      out.collect(r)
    }
  }

  class TemperatureAlert extends KeyedProcessFunction[String, SensorReading, String] {

    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double]))

    lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long]) )

    override def processElement(r: SensorReading,
                                ctx: KeyedProcessFunction[String,
                                  SensorReading, String]#Context,
                                out: Collector[String]): Unit = {

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

    override def onTimer(ts: Long, ctx: KeyedProcessFunction[String, SensorReading,
      String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("传感器报错了！")
      currentTimer.clear()
    }
  }

  class MyAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
    override def extractTimestamp(element: SensorReading): Long = {
      element.timestamp
    }
  }

  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(r: SensorReading): String = r.id
  }

  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = {
      value.temperature > 25
    }
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
        curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )

        val curTime = Calendar.getInstance.getTimeInMillis

        // emit new SensorReading
        // 发射新的传感器数据, 注意这里srcCtx.collect
        curFTemp.foreach( t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

        // wait for 100 ms
        Thread.sleep(100)

      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

}