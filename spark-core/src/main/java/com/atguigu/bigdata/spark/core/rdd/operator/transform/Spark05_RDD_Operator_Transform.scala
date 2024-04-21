package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - glom
    // glom ： 将每个分区中的数据组成内存数组，相当于把散的数据整合，正好与flatMap相反（把整体打散）
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach( arr => println(arr.mkString(", ")))

    sc.stop()
  }

}
