package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - glom
    // 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    val mapRDD: RDD[Int] = glomRDD.map(_.max)

    println(mapRDD.collect().sum)
    sc.stop()
  }

}
