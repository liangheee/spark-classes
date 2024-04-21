package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:27
 */
object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persists")
    val sc = new SparkContext(sparkConf)
    // TODO 问题说明：由于RDD弹性分布式数据集是不会保存数据的，仅仅是逻辑的封装
    // TODO 下面的操作我们仅仅是复用了前面的对象，并没有数据的保存复用
    val list = List("Hello Scala,Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatMapRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word,1)
    })

    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)

    println("************************************")

    val groupByKeyRDD = mapRDD.groupByKey()

    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
