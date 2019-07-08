package com.atguigu.spark.mysql

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Mysql_Reader {

  def main(args: Array[String]): Unit = {

    // 创建 spark 配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkMysql").setMaster("local[*]")
    // 创建 sparkContext driver 类型
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 配置 mysql参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    // 创建 JdbcRDD

    val jdbcRDD: JdbcRDD[(Int, String)] = new JdbcRDD(
      sparkContext,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },

      // sql 是在 executor 端 进行执行的
      "select * from `rdduser` where id >= ? and id <= ?",
      2001,
      2003,
      // 分三个分区
      3,
      (res) =>{
        // println((res.getInt(1), res.getString(2)))
        (res.getInt(1), res.getString(2))
      }
    )

    println(jdbcRDD.count())

    jdbcRDD.foreach(println)
    sparkContext.stop()

  }

}
