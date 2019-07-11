package com.bigdata.sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver

object SparkStreaming_DefineReceiver {

  def main(args: Array[String]): Unit = {

    // 创建配置文件
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkStreamingWordCount").setMaster("local[*]")
    // 创建 StreamContext 环境
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    val lineStream: ReceiverInputDStream[String] = streamingContext.receiverStream(new DefineReceiver("hadoop102",9999))

    val wordDStream: DStream[String] = lineStream.flatMap(line => line.split(" "))

    val tupleDStream: DStream[(String, Int)] = wordDStream.map(word => {
      (word, 1)
    })

    val wordSumDStream: DStream[(String, Int)] = tupleDStream.reduceByKey(_ + _)


    wordSumDStream.print()

    // 单独启动一个线程 采集器
    streamingContext.start()


    // 阻止 driver 停止
    streamingContext.awaitTermination()
  }

}

class DefineReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_SER_2) {


  private var socket: Socket = _


  def receiver(): Unit = {

    // 建立一个socket 管道
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException => return
    }

    // 得到一个输入流的管道 输入的是字节流
    val inputStream = socket.getInputStream
    // 字节流 进行 字符流转换
    val streamReader = new InputStreamReader(inputStream, "UTF-8")
    // 字符流进行缓冲处理
    val reader = new BufferedReader(streamReader)

    var line: String = ""

    while ((line = reader.readLine()) != null) {

      if ("==END==" == line) {
        return
      } else {
        store(line)
      }
    }

  }

  override def onStart(): Unit = {

    new Thread("Define Socket Receiver") {

      override def run(): Unit = {
        // receiver 结束 则 run 方法结束  run 结束则线程结束
        receiver()
      }
    }.start()

  }


  override def onStop(): Unit = ???


}
