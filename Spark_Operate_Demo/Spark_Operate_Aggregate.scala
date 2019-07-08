package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operate_Aggregate {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val numRDD: RDD[Int] = sc.makeRDD(1 to 10 ,2)
    //                10
    // 10, 1, 2, 3 => 16
    // 10, 4, 5, 6 => 25
    // aggregate零值在分区内和分区间都会起作用
    // aggregateByKey零值只在分区内起作用
    //        val result: Int = numRDD.aggregate(10)(_+_, _+_)
    val result: Int = numRDD.fold(10)(_+_)
    println("result = " + result)
    sc.stop()

  }
}
