package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-28 17:11
 */
object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6),3)

    // 10 + 13 + 17 = 40
    // aggregateByKey : 初始值只会参与分区内计算
    // aggregate: 初始值既要参与分区间计算,又要参与分区内计算
//    val value: Int = rdd.aggregate(10)(_ + _, _ + _)
    val value = rdd.fold(10)(_ + _)
    println(value)


    sc.stop()
  }

}
