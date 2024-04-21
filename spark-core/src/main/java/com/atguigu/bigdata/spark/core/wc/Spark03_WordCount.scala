package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-25 22:44
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架环境
    // TODO 建立与Spark框架环境的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 按照相同的key进行聚合
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
