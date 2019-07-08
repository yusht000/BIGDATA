package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Operate_CountByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")


    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("a",1),("a",1),("b",2)))

    // 单独对 key 键的统计
    val stringToLong: collection.Map[String, Long] = dataRDD.countByKey()
    println(stringToLong)

  }
}
