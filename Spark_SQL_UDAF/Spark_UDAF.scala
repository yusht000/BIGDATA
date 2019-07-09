package com.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object Spark_UDAF {

  def main(args: Array[String]): Unit = {

    val spark_SQLConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark_SQL")

    val spark: SparkSession = SparkSession.builder().config(spark_SQLConf).getOrCreate()

    // For implicit conversions like converting  RDDS to DataFrames
    import spark.implicits._

    val dataRDD = spark.sparkContext.makeRDD(List(("zhangsan",20L),("lisi",30L),("wangwu",40L)))

    val dataFrame: DataFrame = dataRDD.toDF("name","age")

    val unit: Unit = dataFrame.createTempView("user")

    val ageUDAF = new AvgAgeUDAF
    spark.udf.register("ageUDAF",ageUDAF)

    spark.sql("select ageUDAF(age) from user").show()

    spark.stop()
  }

}

class AvgAgeUDAF extends UserDefinedAggregateFunction {


  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("cnt", LongType)
  }

  override def dataType: DataType = {
    DoubleType
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {

    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 相同时 executor 间的数据合并
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  // 不同Executor间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }

}