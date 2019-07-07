package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operate_Demo {

  def main(args: Array[String]): Unit = {

    // TODO  创建 Driver 类
    val sparkConf: SparkConf = new SparkConf().setAppName("SparkOperateDemo").setMaster("local[*]")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    // TODO  将数据进行导入 创建 rdd
    val lineRDD: RDD[String] = sparkContext.textFile("input/agent.log")
    // TODO  将数据转化 省份和 广告 的数据结构
    //  (省份-广告,1) (省份-广告,1) (省份-广告,1)
    val priAdsToOneMapRDD: RDD[(String, Int)] = lineRDD.map {
      datas => {
        val strArr: Array[String] = datas.split(" ")
        if (!strArr(1).isEmpty && !strArr(4).isEmpty) {
          (strArr(1) + "_" + strArr(4), 1)
        }else{
          (-1+"_"+ -1,0)
        }
      }
    }
    //priAdsToOneMapRDD.collect().foreach(println)

    // TODO 将每个省份中 相同广告的数量进行统计
    // (河北省-广告1,1) (河北省-广告1,1)=> (河北省-广告1,2)
    // 按照 key 相同进行分组： 参加运算时value 这两个值进行运算
    val priAdsToSumRDD: RDD[(String, Int)] = priAdsToOneMapRDD.reduceByKey(_+_)

    //priAdsToSumRDD.collect().foreach(println)

    // TODO 将每个省的广告统一放在一起
    // (河北省-广告1,2),(河北省-广告2,4)=>(河北省,(广告1,2),(广告2,4))

    val priToAdsSumRDD: RDD[(String, Iterable[(String, Int)])] = priAdsToSumRDD.map {
      case (key, value) => {
        val arrs: Array[String] = key.split("_")
        (arrs(0), (arrs(1), value))
      }
    }.groupByKey()

    //priToAdsSumRDD.collect().foreach(println)

    // TODO 进行排名 然后 获取每个省份广告数量前三名

    val priAdsTop3RDD: RDD[(String, List[(String, Int)])] = priToAdsSumRDD.mapValues {
      datas =>
        datas.toList.sortWith {
          (left, right) => {
            left._2 > right._2
          }
        }.take(3)
    }

    priAdsTop3RDD.collect().foreach(println)

    sparkContext.stop()
  }
}
