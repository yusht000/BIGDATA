package com.atguigu.spark.mysql

import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_Mysql_Writer3 {

  def main(args: Array[String]): Unit = {

    // 创建 spark 配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkMysql").setMaster("local[*]")
    // 创建 sparkContext driver 类型
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 数据库的链接配置
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val passWd = "000000"

    //  向Mysql写入数据
    val dataRDD: RDD[(Int, String, Int)] = sparkContext.makeRDD(List((1, "zs", 22), (2, "wu", 33), (3, "ss", 44)))

    // 使用分区进行遍历
    // 1000完条数据 如果两个区
    // IO 以分区为单位 进行连接 提高性能
    dataRDD.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into `rdduser`(id,name,age)values(?,?,?)"
      val psStat = connection.prepareStatement(sql)
      // 发往 executor code 去执行

      datas.foreach {
        case (id, name, age) => {
          psStat.setInt(1, id)
          psStat.setString(2, name)
          psStat.setInt(3, age)
          psStat.executeUpdate()

        }
      }
      psStat.close()
      connection.close()
    })

    sparkContext.stop()
  }

}
