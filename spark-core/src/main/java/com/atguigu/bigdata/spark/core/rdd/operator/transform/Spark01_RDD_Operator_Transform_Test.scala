package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 14:14
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")

    // 从服务器日志数据apache.log中获取用户请求URL资源路径
    val mapRDD: RDD[String] = rdd.map(
      lines => {
        val splits = lines.split(" ")
        splits(6)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
