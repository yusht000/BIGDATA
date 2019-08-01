package com.flink.process.sideoutput


import com.flink.bean
import com.flink.bean.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
object StreamingJob_Output {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val monitorReadings: DataStream[SensorReading] = readings.process(new FreezingMonitor)

    monitorReadings.getSideOutput(new OutputTag[String]("freezing-alarms")).print()

    env.execute("Flink Streaming Scala API Skeleton")
  }


  class FreezingMonitor extends ProcessFunction[bean.SensorReading, bean.SensorReading] {

    lazy val freezingAlarmOutput: OutputTag[String] =
      new OutputTag[String]("freezing-alarms")

    override def processElement(
                                 value: bean.SensorReading,
                                 ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                 out: Collector[SensorReading]
                               ): Unit = {

      // emit freezing alarm if temperature
      if (value.temperature < 32.0) {
        ctx.output(freezingAlarmOutput, s"Freezing_Alarm_for${value.id}")
      }

      // forward all readings to the regular output
      out.collect(value)
    }
  }

}
