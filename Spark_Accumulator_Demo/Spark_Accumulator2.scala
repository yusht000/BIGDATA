package com.stage2.sparkstructure

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Accumulator2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Accumulator").setMaster("local[*]")

    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val wordRDD: RDD[String] = sparkContext.makeRDD(List("hello","scala","hbase","spark"))

    // 创建累加器
    val accumulator = new DefineAccumulator
    // 累加的注册
    sparkContext.register(accumulator,"blackName")

    wordRDD.foreach(
      word=>{
        // 操作累加器
        accumulator.add(word)
      }
    )
    // 访问累加
    println(accumulator.value)
    sparkContext.stop()
  }
}

// 自定义过滤 单词包含h 字母


class DefineAccumulator extends AccumulatorV2[String, util.HashSet[String]] {

  val blackNameSet = new util.HashSet[String]()

  // 是否初始化
  override def isZero: Boolean = {
    blackNameSet.isEmpty
  }

  // 复制累加器
  override def copy(): AccumulatorV2[String, util.HashSet[String]] = {
    new DefineAccumulator
  }

  // 将累加器 进行重置
  override def reset(): Unit = {

    blackNameSet.clear()
  }

  // 累加数据
  override def add(word: String): Unit = {

    if (word.contains("h")) {
      blackNameSet.add(word)
    }
  }

  // 分区间 进行数据的融合
  override def merge(other: AccumulatorV2[String, util.HashSet[String]]): Unit = {
    blackNameSet.addAll(other.value)
  }

  // 获取累加器的值
  override def value: util.HashSet[String] = {
    blackNameSet
  }


}