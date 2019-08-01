package com.flink.process.StatefulOpera

import java.{lang, util}

import com.flink.bean
import com.flink.bean.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object StatefulOpera {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val keyedStream: KeyedStream[SensorReading, String] = readings.keyBy(sensor => sensor.id)

    keyedStream.flatMap(new TemperatureAlertFunction(1.7))

    keyedStream.flatMapWithState[(String, Double, Double), Double] {
      case (sensor: SensorReading, None) => (List.empty, Some(sensor.temperature))
      case (sensor: SensorReading, lastTemp: Some[Double]) => {
        val tempDiff: Double = (sensor.temperature - lastTemp.get).abs
        if (tempDiff > 1.7) {
          (List((sensor.id, sensor.temperature, tempDiff)), Some(sensor.temperature))
        } else {
          (List.empty, Some(sensor.temperature))
        }
      }
    }

    env.execute("Flink Streaming Scala API Skeleton")
  }

  class TemperatureAlertFunction(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {


    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {

      val lastTempDes: ValueStateDescriptor[Double] = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])

      // 通过句柄获取上一次的状态值
      lastTempState = getRuntimeContext.getState[Double](lastTempDes)

    }

    override def flatMap(
                          value: SensorReading,
                          out: Collector[(String, Double, Double)]
                        ): Unit = {
      val lastTemp: Double = lastTempState.value()

      val tempDiff: Double = (value.temperature - lastTemp).abs

      if (tempDiff > threshold) {
        out.collect((value.id, value.temperature, tempDiff))
      }

      this.lastTempState.update(value.temperature)
    }

  }


  // 一个对每一个并行实例的超过阈值的温度的计数程序
  class HighTempCounter(val threshold: Double)
    extends RichFlatMapFunction[SensorReading, (Int, Long)]
      with ListCheckpointed[java.lang.Long] {


    // index Of the subtask
    private lazy val subtaskIdx: Int = getRuntimeContext.getIndexOfThisSubtask

    // local count variable
    private var highTempCnt = 0L

    override def flatMap(
                          value: SensorReading,
                          out: Collector[(Int, Long)]
                        ): Unit = {

      if (value.temperature > threshold) {

        highTempCnt += 1

        out.collect((subtaskIdx, highTempCnt))
      }

    }

    override def snapshotState(
                                checkpointId: Long,
                                timestamp: Long
                              ): util.List[lang.Long] = {

      java.util.Collections.singletonList(highTempCnt)

    }


    override def restoreState(
                               state: java.util.List[java.lang.Long]
                             ): Unit = {
      highTempCnt = 0L

      for (elem <- state.asScala) {
        highTempCnt += elem
      }
    }
  }

}
