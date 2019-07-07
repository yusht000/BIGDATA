package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operate_Sample {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 创建 新 rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 10 , 2)

    // 三个 参数 ：
    // 第一个参数 ： 数据有无 放回的抽样
    // 第二个参数 ： 数据的泊松分布 计算概率密度
    // 第三个参数 ： 种子源 如果种子相同 则数据随机取样概率是相同 每次获取数据是相同的 反之
    val falseRDD: RDD[Int] = rdd.sample(false,0.4)

    val trueRDD: RDD[Int] = rdd.sample(true,0.5,1)

    falseRDD.collect().foreach(println)
    trueRDD.collect().foreach(println)
    sc.stop()

  }
}
