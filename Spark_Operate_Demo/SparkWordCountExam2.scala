package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkWordCountExam2 {

  def main(args: Array[String]): Unit = {

    // 额外 配置文件配置信息
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("WC").setMaster("local")
    // 获取 spark 上下文环境 变量
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    // 将文件中数据一行一行读入
    val lineRDD: RDD[String] = sparkContext.textFile("input")
    // 将一行一行数据进行扁平化 变成单个数据单词 来使用
    val wordToRDD: RDD[String] = lineRDD.flatMap(line=>line.split(" "))

    // 将一个一个 单词 进行类型结构转化
    val wordToOneRDD: RDD[(String, Int)] = wordToRDD.map(word=>{(word,1)})

    // 利用 spark 中 封装方法
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = wordToOneRDD.groupByKey()

    val value: RDD[(String, Int)] = groupByKeyRDD.mapValues(datas=>datas.toList.sum)

    value.collect().foreach(println)
    sparkContext.stop()

  }
}
