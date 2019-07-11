package com.bigdata.sparkstreaming


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreaming_Kafka {

  def main(args: Array[String]): Unit = {

    // 创建上下文环境
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[*]")

    // 创建 上下文 环境对象 设置为批次采集的时间间隔 每次间隔3秒钟  采集数据
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(3))

    //val lineStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102",9999)

    // 设置kafka 消费数据源 形成DStream  启动了两个线程 EventThread SendEventThread .start()
    // ping 心跳方法调用
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      streamingContext,
      Map(
        // 配置连接zookeeper 配置文件
        ConsumerConfig.GROUP_ID_CONFIG->"bigDataGroup",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->"org.apache.kafka.common.serialization.StringDeserializer",
        "zookeeper.connect" -> "hadoop102:2181"
      ),
      Map(
        "bigDataSparkStreaming"->3
      ),
      StorageLevel.MEMORY_AND_DISK_SER_2
    )


    // 进行进行数据转化处理
    val lineStream: DStream[String] = kafkaDStream.map {
      case (k, v) => v
    }


    val wordDStream: DStream[String] = lineStream.flatMap(line=>line.split(" "))

    val tupleDStream: DStream[(String, Int)] = wordDStream.map(word=>{(word,1)})

    val wordSumDStream: DStream[(String, Int)] = tupleDStream.reduceByKey(_+_)

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
