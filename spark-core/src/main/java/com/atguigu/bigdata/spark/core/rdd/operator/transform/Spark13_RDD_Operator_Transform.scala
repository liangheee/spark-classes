package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark13_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - 双Value类型
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))
    val rdd3 = sc.makeRDD(List("3","4","5","6"))


    // 交集： [3,4]
    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd2)
    // rdd中数据类型不一致，不能进行交集、并集和差集操作！！！！
//    val intersectionRDD: RDD[Int] = rdd1.intersection(rdd3)
    println(intersectionRDD.collect().mkString(", "))

    // 并集 1,2,3,4,5,6,3,4
    val unionRDD: RDD[Int] = rdd1.union(rdd2)
    println(unionRDD.collect().mkString(", "))

    // 差集
    val substractRDD: RDD[Int] = rdd1.subtract(rdd2)
    println(substractRDD.collect().mkString(", "))

    // 拉链 1-3 2-4 3-5 4-6
    val zipRDD: RDD[(Int,Int)] = rdd1.zip(rdd2)
    println(zipRDD.collect().mkString(", "))
    // zip拉链可以处理RDD不同元素类型
    val zipRDD2: RDD[(Int,String)] = rdd1.zip(rdd3)
    println(zipRDD2.collect().mkString(", "))

    sc.stop()
  }

}
