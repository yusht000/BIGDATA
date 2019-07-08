package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Define_Function {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    // 对RDD逻辑进行封装时，为了防止序列化异常，所以封装类都要是序列化接口
    val s = new Search("h")

    //val filterRDD: RDD[String] = s.getMatch1(rdd)
    val filterRDD: RDD[String] = s.getMatch2(rdd)

    filterRDD.foreach(println)

    sc.stop()

  }
}

//class Search(query:String) extends Serializable {
class Search(query:String)  {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
    // 传递属性 实际调用是 ： this.query
    // 需要将 search 序列化 才能进行传递
    s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    // isMatch 是 search 类中 的方法
    // 实际调用的方法 是 this.isMatch
    // 是 search 类中方法 在 executor 端执行 需要将 search 类进行序列化
    rdd.filter(isMatch)
  }

  //过滤出包含字符串的RDD
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    // q : 是一个新的序列化字符类型 与 整个search  类无关 可以进行 序列化传递
    val q : String = query
    rdd.filter(x => x.contains(q))
  }

}


