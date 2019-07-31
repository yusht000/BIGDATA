package com.flink

import com.flink.bean.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob_WaterMarker {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 从调用时刻 给env 创建的每个一Stream 追加时间特征
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val assigTimestampStream: SingleOutputStreamOperator[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new MyAssiger)


    env.execute("Flink Streaming Scala API Skeleton")
  }

  // 自定义 WaterMarker 抽取
  class MyAssiger extends AssignerWithPeriodicWatermarks[SensorReading] {

    val bound: Long = 60 * 1000L // 延时为 1 分钟
    var maxTs: Long = Long.MinValue // 观察到的最大时间戳

    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs - bound)
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
      maxTs = maxTs.max(element.timestamp)
      element.timestamp
    }
  }

  // 预估计 数据流中的时间 最大延迟时间
  class SensorTimeAssiger
    extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
    override def extractTimestamp(element: SensorReading): Long = element.timestamp
  }

  // 根据 流的id 给具体的流插入水印标记
  class PunctuatedAssiger extends AssignerWithPunctuatedWatermarks[SensorReading] {

    val bound: Long = 60 * 1000L

    override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long):
    Watermark = {

      if (lastElement.id == "sensor_1") {
        new Watermark(extractedTimestamp - bound)
      } else {
        null
      }
    }

    override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long):
    Long = element.timestamp


  }

}
