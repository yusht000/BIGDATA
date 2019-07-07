package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Operate_Coalesce {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 创建 新的 rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 16 ,4)
    // 改变分区
    val coalRDD: RDD[Int] = rdd.coalesce(3)

    // 将每一个分区内数据打印出来
    // 当 第三个参数 为 false 的分区 不会使用 shuffle 只是分区的合并
    // 当 第三个参数 为 true 的时候 就会使用shuffle 避免的是数据倾斜
    val mapIndexRDD: RDD[(Int, Int)] = coalRDD.mapPartitionsWithIndex {
      (index, value) => {
        value.map((index, _))
      }
    }

    mapIndexRDD.collect().foreach(println)

    sc.stop()

  }
}
