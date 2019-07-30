package com.flink

import com.flink.bean.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

object StreamingJob_ProcessFuction {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val readings: DataStreamSource[SensorReading] = env.addSource(new SensorSource)

    readings.keyBy(_.id)
      .process(new TempIncreaseAlertFunction)
    env.execute("Flink Streaming Scala API Skeleton")
  }

  class TempIncreaseAlertFunction extends KeyedProcessFunction[String, SensorReading, String] {

    // 保存上一个传感器的温度值
    lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", Types.of[Double])
    )

    // 保存注册的定时器的时间戳
    lazy val value: ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )


    override def processElement(
                                 value: SensorReading,
                                 ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                 out: Collector[String]
                               ): Unit = {

    }


  }

}
