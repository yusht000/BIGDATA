package com.bigdata.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_DataSet_DF {

  def main(args: Array[String]): Unit = {

    // 配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    // 准备环境对象
    //val spark = new SparkSession(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 将RDD, DataFrame, DataSet进行转换时，需要隐式转换规则
    // 此时spark不是包名，是SparkSession对象名称
    import spark.implicits._


    val dataRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan", 30), (2,"lisi", 40), (3,"wangwu", 20)))
    //增加结构信息 转化为 dataFrame
    val dFrame: DataFrame = dataRDD.toDF("id","name","age")

    // 添加 类型信息 就会得到DataSet
    val dSet: Dataset[User] = dFrame.as[User]

    // 转化为dataFrame
    val dFrame2: DataFrame = dSet.toDF()
    // 转化为 rdd
    val rdd: RDD[Row] = dFrame2.rdd

    rdd.foreach(row=>{println(row.getInt(0)+","+row.getString(1))})

    // (Int ,String, Int) => (id,name,age)=>User

    val userRDD: RDD[User] = dataRDD.map {
      case (id, name, age) => {
        // 类型的转化
        User(id, name, age)
      }
    }

    val ds: Dataset[User] = userRDD.toDS()

    //dataSet => rdd
    val rdd2: RDD[User] = ds.rdd
    rdd2.foreach(user=>{println(user.id+","+user.age)})
   spark.stop()
  }
}

case class User(id:Long,name:String,age:Long)

