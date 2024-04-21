package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter
    // filter：将满足条件的数据过滤留下，不满足条件的丢弃
    // 分区不变，但是分区内的数据可能不均衡，生产环境下，很有可能出现数据倾斜的现象
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val filterRDD = rdd.filter(_ % 2 == 0)

    filterRDD.saveAsTextFile("output")
//    filterRDD.collect().foreach(println)

    sc.stop()
  }

}
