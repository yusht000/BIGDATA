package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Operate_GroupBy {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(num=>num)


    // 中间有shuffle 落盘 操作 将原来顺序 都打乱
    groupByRDD.collect().foreach(println)

    sc.stop()
  }
}
