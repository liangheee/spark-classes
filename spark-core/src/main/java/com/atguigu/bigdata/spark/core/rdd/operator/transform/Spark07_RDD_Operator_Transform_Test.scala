package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark07_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - filter
    // 从服务器日志数据apache.log中获取2015年5月17日的请求路径
    val rdd = sc.textFile("datas/apache.log")

    val filterRDD: RDD[String] = rdd.filter(
      line => {
        val datas = line.split(" ")
        val date = datas(3)
        date.startsWith("17/05/2015")
      }
    )

    filterRDD.collect().foreach(println)

    sc.stop()
  }

}
