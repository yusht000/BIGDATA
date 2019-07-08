package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark14_Operate_CountByValue {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("hello",1),("hello",1),("hello",2),("spark",1),("hbase",2)))

    val strRDD: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Scala"))

    val stringToLong: collection.Map[String, Long] = strRDD.countByValue()

    println(stringToLong)

    val tupleToLong: collection.Map[(String, Int), Long] = dataRDD.countByValue()

    println(tupleToLong)
  }
}
