package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-25 22:44
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark框架环境
    // TODO 建立与Spark框架环境的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1、从指定文件夹中读取数据
    val lines: RDD[String] = sc.textFile("datas")

    // 2、进行扁平化操作
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 3、进行分组操作
    val groupWord: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 4、进行map操作
    val wordToCount: RDD[(String, Int)] = groupWord.map {
      case (word, list) => (word, list.size)
    }

    // 5、收集结果，打印输出到控制台
    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples.foreach(println)


    // TODO 关闭连接
    sc.stop()
  }

}
