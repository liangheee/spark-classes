package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:39
 */
object Spark02_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Dependencies")
    val sc = new SparkContext(sparkConf)

    // TODO 通过dependencies方法进行依赖关系的打印
    // TODO OneToOneDependency  NarrowDependency 窄依赖
    val rdd = sc.textFile("datas/word.txt")
    println(rdd.dependencies)
    println("********************************")
    val flatMapRDD = rdd.flatMap(_.split(" "))
    println(flatMapRDD.dependencies)
    println("********************************")
    val mapRDD = flatMapRDD.map((_, 1))
    println(mapRDD.dependencies)
    println("********************************")
    // TODO ShuffleDependency Dependency  宽依赖
    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)
    println(reduceByKeyRDD.dependencies)
    println("********************************")
    reduceByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
