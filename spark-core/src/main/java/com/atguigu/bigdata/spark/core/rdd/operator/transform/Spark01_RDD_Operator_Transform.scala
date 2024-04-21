package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 14:14
 */
object Spark01_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 转换函数
    def mapFunction(n: Int): Int = {
      n * 2
    }

//    val mapRDD: RDD[Int] = rdd.map(mapFunction)
    val mapRDD = rdd.map(_*2)

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
