package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:27
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persists")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala,Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatMapRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatMapRDD.map((_, 1))

    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)

    println("************************************")

    val list1 = List("Hello Scala,Hello Spark")

    val rdd1 = sc.makeRDD(list1)

    val flatMapRDD1 = rdd1.flatMap(_.split(" "))

    val mapRDD1 = flatMapRDD1.map((_, 1))

    val groupByKeyRDD = mapRDD1.groupByKey()

    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
