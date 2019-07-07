package com.atguigu.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark09_OperateKV_PartitionBy {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_Partitions").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // 创建新的 rdd
    val rdd: RDD[Int] = sc.makeRDD(1 to 10)
    // k-v 类型 rdd
    // RDD当类型为K-V类型时，可以采用隐式转换自动变成PairRDDFunctions类型，然后调用其中的方法
    /*
       implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
          new PairRDDFunctions(rdd)
        }
    *
    */
    //val reduceByKeyRDD: RDD[(Int, Int)] = rdd.map((_,1)).reduceByKey(_+_)

    // 算子 - partitionBy

    val newRDD = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    // Partitonner 按照 key 重新分区
    val partitionRDD: RDD[(Int, String)] = newRDD.partitionBy(new HashPartitioner(2))


    val mapRDD: RDD[(Int, (Int, String))] = partitionRDD.mapPartitionsWithIndex {
      (index, datas) => {
        datas.map((index, _))
      }
    }
    mapRDD.collect().foreach(println)

    sc.stop()

  }
}
