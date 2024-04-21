package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:39
 */
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Dependencies")
    val sc = new SparkContext(sparkConf)

    // TODO 通过toDebugString方法进行血缘关系的打印
    val rdd = sc.textFile("datas/word.txt")
    println(rdd.toDebugString)
    println("********************************")
    val flatMapRDD = rdd.flatMap(_.split(" "))
    println(flatMapRDD.toDebugString)
    println("********************************")
    val mapRDD = flatMapRDD.map((_, 1))
    println(mapRDD.toDebugString)
    println("********************************")
    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)
    println(reduceByKeyRDD.toDebugString)
    println("********************************")
    reduceByKeyRDD.collect().foreach(println)

    sc.stop()

  }

}
