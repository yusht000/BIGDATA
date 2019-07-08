package com.stage2.sparkstructure

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Accumulator1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark_Accumulator").setMaster("local[*]")

    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(1 to 3)

    // 将 driver 内存中数据进行累加操作
    // reduce 能够发生shuffler 落盘操作
    //val i: Int = rdd.reduce(_+_)

    // 下面代码 替代 reduce 操作

    // driver 声明的变量
    var sum = 0;

    // 进行分布式计算
    rdd.foreach(
      // executor code 代码
      num=>{
        sum = sum + num
      }
    )

    // driver 内存中变量
    // executor 中代码没有返回值
    // 所有 sum 仍然为零
    println("sum  = "+ sum)


    // 将数据 返回spark
    // 通过声明和使用累加器 来实现此功能
    val total: LongAccumulator = sparkContext.longAccumulator("total")

     rdd.foreach(
       num=>{
         // executor 中使用total
         total.add(num)
       }
     )
    println(total)

    sparkContext.stop()
  }
}
