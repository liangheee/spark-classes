package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-28 17:11
 */
object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

//    val rdd = sc.makeRDD(List(1, 1, 1, 4), 2)

    val rdd = sc.makeRDD(List(
      ("a",1),("b",2),("a",3)
    ))

//    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
//    println(intToLong)

    // 这是计算次数,与kv中的v没有任何关系
    val stringToLong: collection.Map[String, Long] = rdd.countByKey()
    println(stringToLong)


    sc.stop()
  }

}
