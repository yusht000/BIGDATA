package com.bigdata.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object SparkStreaming_UpdateState {


  def main(args: Array[String]): Unit = {

    // 创建上下文环境
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[*]")

    // 创建 上下文 环境对象 设置为批次采集的时间间隔 每次间隔3秒钟  采集数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    streamingContext.sparkContext.setCheckpointDir("ckeckpoint")
    // 从指定的端口号中 获取数据
    // 这行代码 相等于 客户端 连接 netcat 这个服务 相互传递了一个引用 通过装状态的变化 来向服务器读取数据
    val lineStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    // 将一行数据进行扁平化操作
    val wordDStream: DStream[String] = lineStream.flatMap(line=>line.split(" "))

    // 将单词转换结构
    val tupleDStream: DStream[(String, Int)] = wordDStream.map(word=>{(word,1)})

    // 进行聚合
    // reduceByKey : 无状态操作, 只是对 当前RDD中数据有效 无法对多个采集周期的数据进行统计
    //val wordSumDStream: DStream[(String, Int)] = tupleDStream.reduceByKey(_+_)

    // 有状态的数据操作 需要设置检查点操作； 将数据状态进行保存
    val wordSumDStream: DStream[(String, Long)] = tupleDStream.updateStateByKey[Long] {
      (valSeq: Seq[Int], buffer: Option[Long]) => {
        val sum: Long = buffer.getOrElse(0L) + valSeq.sum
        Option(sum)
      }
    }

    wordSumDStream.print()

    // 单独启动一个线程 采集器
    // 释放资源
    // SparkStreaming的采集器需要长期执行，所以不能停止
    // SparkStreaming的采集器需要明确启动
    streamingContext.start()


    // 阻止 driver 停止
    // Driver 程序不能停止 需要等待采集器的执行结束才能结束
    streamingContext.awaitTermination()


  }

}
