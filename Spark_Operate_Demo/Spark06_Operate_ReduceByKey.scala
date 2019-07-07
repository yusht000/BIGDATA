package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Operate_ReduceByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 算子 - reduceByKey
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3)))

    val unit: RDD[(String, Int)] = rdd.reduceByKey(_+_)

  }
}
