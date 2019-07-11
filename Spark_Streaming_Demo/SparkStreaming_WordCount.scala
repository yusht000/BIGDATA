package com.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming_WordCount {

  def main(args: Array[String]): Unit = {

    // 创建配置文件
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[*]")
    // 创建 StreamContext 环境
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    val lineStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    val wordDStream: DStream[String] = lineStream.flatMap(line=>line.split(" "))

    val tupleDStream: DStream[(String, Int)] = wordDStream.map(word=>{(word,1)})

    val wordSumDStream: DStream[(String, Int)] = tupleDStream.reduceByKey(_+_)


    wordSumDStream.print()

    // 单独启动一个线程 采集器
    streamingContext.start()


    // 阻止 driver 停止
    streamingContext.awaitTermination()
  }

}
