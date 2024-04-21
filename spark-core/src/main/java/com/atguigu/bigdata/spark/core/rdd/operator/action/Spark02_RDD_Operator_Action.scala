package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-28 17:11
 */
object Spark02_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // TODO - 行动算子
    // reduce: 先聚合分区内的数据,再聚合分区间的数据
    val i: Int = rdd.reduce(_ + _)
    println(i)

    // collect : 方法会将不同分区的数据按照分区顺序采集到Driver端内存中,形成数组
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(", "))

    // count : 返回rdd中元素的个数
    val cnt = rdd.count()
    println(cnt)

    // first : 返回rdd中的第一个元素
    val first = rdd.first()
    println(first)

    // take(n) : 返回一个由rdd前n个元素组成的数组
    val ints1: Array[Int] = rdd.take(2)
    println(ints1.mkString(", "))

    // takeOrdered(n) : 返回rdd排序后前n个元素组成的数组   (默认是升序排列)
    val rdd1 = sc.makeRDD(List(4, 3, 2, 1))
    val ints2 = rdd1.takeOrdered(2)
    println(ints2.mkString(", "))


    sc.stop()
  }

}
