package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {


  def main(args: Array[String]): Unit = {

    // 设置配置文件参数
    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")

    // 创建spark上下文   环境对象Driver
    val sparkContext: SparkContext = new SparkContext(conf)

    // 创建 抽象对象RDD 计算单元 不可变  借用装饰设计模式  不断添加新的功能 不断产生新的对象
    val newRDD: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4,5))

    // 第二种实现方式 创建出一个 新的RDD 对象  创建对象默认划好 分区
    val newRDD2: RDD[Int] = sparkContext.parallelize(List(1,2,3,4,5))

    // 在Driver 这个类中
    // Driver Code  数据存在driver 这个类中 运行在 这个类所在的内存中
    List(1,2,3,4)

    // 进行是rdd 的转换
    // 调用 newRDD 对象中 mapPartitions 算子  会产生新的rdd  默认产生新的分区
    // 将原来 分区数据 按照一定规则 转变后 进入下一个 RDD 分区当中
    // 但是 这 规则的代码逻辑 不在 driver 这类中执行 而是 封装成一对象函数 发送到 executor 节点上进行执行 代码逻辑
    newRDD.mapPartitions{
      datas => {

        // 规则逻辑代码 executor code
        datas.map(_*2)

      }
    }

    // 执行完的数据还要 发回 driver 类中吗??????
    newRDD.collect().foreach(println)

    sparkContext.stop()

  }
}
