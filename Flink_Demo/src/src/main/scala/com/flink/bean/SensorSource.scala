package com.flink.bean

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.immutable
import scala.util.Random

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

