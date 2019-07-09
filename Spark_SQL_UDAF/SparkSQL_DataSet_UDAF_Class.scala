package com.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


object SparkSQL_DataSet_UDAF_Class {

  def main(args: Array[String]): Unit = {


    // 配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")

    // 准备环境对象
    //val spark = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
    // 此时spark不是包名，是SparkSession对象名称
    import spark.implicits._

    val uDAF: AvgAgeClassUDAF = new AvgAgeClassUDAF
    // 无法向spark 进行注册
    //spark.udf.register("testAge",uDAF)

    val col: TypedColumn[User2, Double] = uDAF.toColumn.name("testAvg")

    val df: DataFrame = spark.read.json("input/user.json")
    // 使用DSL 语法来访问强类型聚合函数
    val ds: Dataset[User2] = df.as[User2]

    ds.select(col).show()

    spark.stop()
  }
}


//声明强类型转换
case class User2(name: String, age: Long)

case class AvgBuff(var total: Long, var count: Long)

class AvgAgeClassUDAF extends Aggregator[User2, AvgBuff, Double] {

  override def zero: AvgBuff = {
    AvgBuff(0L, 0L)
  }

  override def reduce(buffer: AvgBuff, put: User2): AvgBuff = {
    buffer.total = buffer.total + put.age
    buffer.count = buffer.count + 1L
    buffer
  }

  override def merge(b1: AvgBuff, b2: AvgBuff): AvgBuff = {
    b1.total = b1.total + b2.total
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(reduction: AvgBuff): Double = {
    reduction.total.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuff] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

