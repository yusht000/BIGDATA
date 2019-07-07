package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Partitions {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    //  创建 rdd 时
    //  调用 makeRDD 会默认将当前cpu 核数 作为分区数目
    //  分区数目是可以修改
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5),2)

    val mapIndexRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex {
      (index, values) => {
        values.map(value => (index, value))
      }
    }
    mapIndexRDD.collect().foreach(println)

    // textFile 默认是cpu 核心 和 2 最小值
    // 文件分区 调用的是hadoop 的接口 以文件为单位的分区
    // 读取方式 按照行的读取方式 不是按照字节的读取方式
    // totalSize += file.getLen();
    // long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits); 在第一个for 循环的计算字节总长度
    // long splitSize = computeSplitSize(goalSize, minSize, blockSize); 在第二个 for 读取是一个个的文件
    // return Math.max(minSize, Math.min(goalSize, blockSize))
    // minPartitions 参数表示 每个分区存储数据的大小的
    // 总字节数 3 minPartitions 2   3 / 2 = 1 说明每个分区 存放一个字节
    // 总字节数 3   3 / 1  需要3个分区
    val txtRDD: RDD[String] = sc.textFile("input",2)

    val unit: Unit = txtRDD.saveAsTextFile("output")


    sc.stop()

  }

}
