package com.flink

import java.util.Calendar

import com.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.immutable
import scala.util.Random

object StreamingJob_Define_Source {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置流 为eventType 事件流
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val sourceDS: DataStreamSource[SensorReading] = env.addSource(new SensorSource)

    sourceDS.print()


    env.execute("Flink Streaming Scala API Skeleton")
  }
}

class SensorSource extends RichParallelSourceFunction[SensorReading]{


  // 设置数据是否 正常运行
  var running : Boolean = true

  // run 函数连续发送SensorReading数据 使用SourceContext
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()
    // 查找当前运行上下的任务索引
    val taskId = this.getRuntimeContext.getIndexOfThisSubtask

    val tuples: immutable.IndexedSeq[(String, Double)] = (1 to 10).map {
      i => ("sensor_" + (taskId * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    var curFTemp = tuples

    // 无限循环 产生数据流
    while (running){

      // 更新 温度
      curFTemp = curFTemp.map(t=>(t._1,t._2+(rand.nextGaussian()*0.5)))

      // get current time

      val curTime = Calendar.getInstance().getTimeInMillis

      // 发送新的传感器数据

      curFTemp.foreach(t=>ctx.collect(SensorReading(t._1,curTime,t._2)))

      // 休息10s
      Thread.sleep(100)
    }

  }

  // override cancel 函数
  override def cancel(): Unit = {
    running = false
  }
}

