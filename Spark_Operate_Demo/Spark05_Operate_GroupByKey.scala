package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Operate_GroupByKey {


  def main(args: Array[String]): Unit = {



    // 算子 - groupByKey

   // val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "aaa"), (2, "bbb"), (1, "ccc"), (2, "ddd")))

    // key 相同则进行分为一组 形成一个可迭代的集合
   // val groupByKeyRDD: RDD[(Int, Iterable[String])] = rdd.groupByKey()

    //groupByKeyRDD.collect().foreach(println)

   // sc.stop()

  }

}
