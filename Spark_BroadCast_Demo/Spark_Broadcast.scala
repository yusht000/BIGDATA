package com.stage2.sparkstructure

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Broadcast {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SparkBroadcast").setMaster("local[*]")

    val sparkContext: SparkContext = new SparkContext(sparkConf)


    val listRDD1: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 1), ("b", 2), ("c", 2)))

    val listRDD2: RDD[(String, Int)] = sparkContext.makeRDD(List(("a", 4), ("b", 5), ("c", 6)))

    // join 要经过 shuffle
    //(a,(1,4)) listRDD1 要与listRDD2 每一个元素进行比对  是一个笛卡尔积的关系
    //(b,(2,5))
    //(c,(2,6))
    // 如果数据量是很大 会有大量数据的落盘 造成性能大幅度的下降
    val joinRDD: RDD[(String, (Int, Int))] = listRDD1.join(listRDD2)

    //joinRDD.collect().foreach(println)

    // 使用没有 shuffle 阶段 也可以完成任务
    // driver 内存的数据
    // 缺点 ： 有可能 导致内存溢出

    //  当一个 executor(container) 有三个core 的资源 那么 可以分配 三个task任务
    //  driver 中的数据 要对应task 拉取三分
    //  如果 数据1000万 在 executor 要有三千万的数据
    //  如果内存 不足 则导致溢出
    val list = List(("a", 4), ("b", 5), ("c", 6))

    val resRDD: RDD[(Any, Any)] = listRDD1.map {

      // execute coding
      case (k1, v1) => {

        var kk: Any = k1
        var vv: Any = null

        for ((k2, v2) <- list) {
          if (k2 == k1) {
            vv = (v1, v2)
          }
        }
        (kk, vv)
      }
    }
    resRDD.collect().foreach(println)


    // TODO  解决以上的 问题 提出 广播变量来优化
    // TODO  声明广播变量
    // 广播变量为共享变量对应是 一个executor  executor 中任务可以 共同读此变量
    val list2 = List(("a", 4), ("b", 5), ("c", 6))

    val listBC: Broadcast[List[(String, Int)]] = sparkContext.broadcast(list2)

    val resRDD2: RDD[(Any, Any)] = listRDD1.map {

      // execute coding
      case (k1, v1) => {
        var kk: Any = k1
        var vv: Any = null
        // 使用广播变量
        for ((k2, v2) <- listBC.value) {
          if (k2 == k1) {
            vv = (v1, v2)
          }
        }
        (kk, vv)
      }
    }

    resRDD2.collect().foreach(println)

    sparkContext.stop()
  }

}
