package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_OperateKV_AggregateByKey {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 算子 - aggregateByKey
    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)

    // aggregateByKey : 根据key来聚合数据
    // 需要传递两个参数列表
    // 第一个参数列表（零值）
    // 第二个参数列表（分区内计算逻辑，分区间计算逻辑）

    // spark 当中都是 进行两两的递归迭代 来来进行运算

    // 将RDD中的数据分区内取最大值，然后分区间相加
    //rdd.aggregateByKey()((x,y)=>x, (x,y)=>x+y)

    // ("a",3),("a",2),("c",4)
    //    ==> (a,10), (c,10)
    // ("b",3),("c",6),("c",8)
    //    ==> (b, 10), (c,10)

    // ==> (a,10)(b,10)(c,20)

    //val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(10)((x,y)=>{math.max(x,y)}, (a,b)=>{a+b})

    // ("a",3),("a",2),("c",4)
    //    ==> (a,5), (c,4)
    // ("b",3),("c",6),("c",8)
    //    ==> (b, 3), (c,14)
    val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
