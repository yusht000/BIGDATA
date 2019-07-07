package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Operate_Glom {

  def main(args: Array[String]): Unit = {

    // glom 算子 是进行分区分组
    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4,12,13,14,15,12,13,14,15),4)

    // 将 分区内进行分组
    // 每一个分区 生产一个数组

    val glomRDD: RDD[Array[Int]] = rdd.glom()
    // 生成 四个数组
    glomRDD.collect().foreach(datas=>{println(datas)})

    sc.stop()
  }
}
