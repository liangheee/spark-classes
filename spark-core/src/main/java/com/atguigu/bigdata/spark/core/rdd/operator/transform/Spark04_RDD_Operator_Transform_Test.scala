package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark04_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - flatMap
    // 将List(List(1,2),3,List(4,5))进行扁平化操作
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))

    val flatMapRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case other => List(other)
        }
      }
    )

    flatMapRDD.collect().foreach(println)

    sc.stop()
  }

}
