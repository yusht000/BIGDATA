package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_OperateKV_CombineByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 算子 - combineByKey

    // 根据key计算每种key的均值
    // ("a", 88), ("b", 95), ("a", 91)
    // ("b", 93), ("a", 95), ("b", 98)

    // a => List(88, 91, 95) => list.sum / list.siz
    // a => List((88,1),(91,1),(95,1)) => (sum, count) => sum / count
    // b => List(95, 93, 98) => list.sum / list.size

    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)

    // combineByKey需要传递三个参数



    // 第一个参数：转换数据的结构 88 => (88,1)
    // 第二个参数：分区内计算规则
    // 第三个参数：分区间计算规则
    // createCombiner: V => C,        (key,value) value=>V 转换数据结构 C
    // mergeValue: (C, V) => C,        C 作为参数 与相同key中 value 进行运算
    // mergeCombiners: (C, C) => C     分区间相同数据结构 按照key 相同 进行运算

    val resultRDD: RDD[(String, (Int, Int))] = input.combineByKey(
      v => (v, 1),
      (t: (Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t: (Int, Int), t1: (Int, Int)) => {
        (t._1 + t1._1, t._2 + t1._2)
      }
    )

    resultRDD.map{
      case (k, v) => {
        (k, v._1/v._2)
      }
    }.collect().foreach(println)

    sc.stop()
  }
}
