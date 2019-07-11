package com.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_Window {

  def main(args: Array[String]): Unit = {

    // 准备 配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkWindow")

    // 创建上下文环境
    val streamingContext = new StreamingContext(sparkConf,Seconds(3))

    // 使用窗口函数 对多个周期的数据进行采集
    val lineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    // 将采集的数据放在窗口函数中  收集的周期只能是 采集是的整数倍  步长也是采集周期整数倍
    val windowDStream: DStream[String] = lineDStream.window(Seconds(6),Seconds(6))

    //  将数据进行扁平化操作
    val wordDStream: DStream[String] = windowDStream.flatMap(line=>line.split(" "))

    //  将单词进行结构转变
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word=>(word,1))

    //  将单词进行聚合操作
    val resultDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_+_)
    resultDStream.print()

    // 启动数据采集器
    streamingContext.start()

    // driver 不能停止
    streamingContext.awaitTermination()

  }
}
