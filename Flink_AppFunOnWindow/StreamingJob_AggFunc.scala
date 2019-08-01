package com.flink.process.AppFuncOnWindow

import com.flink.bean
import com.flink.bean.SensorSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob_AggFunc {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings: DataStream[bean.SensorReading] = env.addSource(new SensorSource)

    val aggTempDS: DataStream[(String, Double)] = readings.map(sensor => {
      (sensor.id, sensor.temperature)
    })
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .aggregate(new AvgTempFunction)

    aggTempDS.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

  class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{

    override def createAccumulator(): (String, Double, Int) = {("",0.0,0)}


    override def add(
                      value: (String, Double),
                      accumulator: (String, Double, Int)
                    ): (String, Double, Int) = {
      (value._1,value._2+accumulator._2,1+accumulator._3)
    }

    override def getResult(
                            accumulator: (String, Double, Int)
                          ): (String, Double) = {
      (accumulator._1,accumulator._2/accumulator._3)
    }

    // 为什么merge 不同节点上的merge吗
    override def merge(
                        a: (String, Double, Int),
                        b: (String, Double, Int)
                      ): (String, Double, Int) = {
      println("merge is called")
      (a._1,a._2+b._2,a._3+b._3)
    }

  }
}
