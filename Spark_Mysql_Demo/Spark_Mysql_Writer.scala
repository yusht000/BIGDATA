package com.atguigu.spark.mysql

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Mysql_Writer {


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

    dataRDD.foreach {

      // foreach 算子里面的代码是 executor coding需要 发往 executor 去执行
      // 如果 dataRDD 数据量 为1000完条数据
      // jvm => mysql 两个之间会不断建立是IO 连接 性能会很慢
      // 这段代码整体执行效率很低
      case (id, name, age) => {

        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into `rdduser`(id,name,age)values(?,?,?)"
        val psStat = connection.prepareStatement(sql)

        psStat.setInt(1, id)
        psStat.setString(2, name)
        psStat.setInt(3, age)


        psStat.executeUpdate()

        psStat.close()
        connection.close()
      }
    }
    sparkContext.stop()
  }

}
