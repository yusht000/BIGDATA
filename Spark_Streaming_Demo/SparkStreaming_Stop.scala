package com.bigdata.sparkstreaming


import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming_Stop {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("SparkStreaming_Stop").setMaster("local[*]")

    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 相当 获取远方 进程的一个 引用
    val lineDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop102", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map(word => (word, 1))

    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    wordToSumDStream.print()


    new Thread(
      // 匿名内部类的实现
      new Runnable {

        override def run(): Unit = {

          while(true){

            try {
              // 线程在这里进行休眠操作
              println("我休息啦 不工作啦")
              val string = Thread.currentThread().getName.toString
              println(string)
              Thread.sleep(Integer.MAX_VALUE)
              println("我开始工作啦")
            } catch {
              case _: Exception => println("xxxxx")
            }

            val fileSystem: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"),new Configuration(),"root")

            val state = streamingContext.getState()
            // 通过监听 状态的变化 来停止 streamingContext 采集停止
            if(state==StreamingContextState.ACTIVE){
              val bool: Boolean = fileSystem.exists(new Path("hdfs://hadoop102/9000/sparkSteaming"))

              if(bool){
                streamingContext.stop(true,true)
                System.exit(0)
              }
            }
          }
        }
      }
    ).start()


    // 释放资源
    // SparkStreaming 的采集器 进行长期的采集
    streamingContext.start()
    // Driver 程序不能 终止 需要等待采集的执行结束才能 终止

    streamingContext.awaitTermination()
  }
}
