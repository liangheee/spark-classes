package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {
    // 1、创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2、执行业务操作
    // 读取文件内容，并且读取文件名称
    // textFile方法读取文件是 按行为单位进行读取
    // wholeTextFiles方法读取文件是 按文件为单位进行读取，读取的结果是一个元组，第一个元素为文件名称，第二个元素为文件内容
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)

    // 关闭环境
    sc.stop()




  }

}
