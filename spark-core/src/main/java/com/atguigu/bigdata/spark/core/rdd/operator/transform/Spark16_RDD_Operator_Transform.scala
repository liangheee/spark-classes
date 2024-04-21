package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark16_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型) reduceByKey
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",4)))

    // reduceByKey：相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark是基于scala开发的，所以它的聚合也是两两聚合
    // 【1，2，3】
    // 【3，3】
    // 【6】
    // reduceByKey中如果key的数据只有一个，那么不会参与运算
    val newRDD = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x=$x, y=$y")
      x + y
    })
    newRDD.collect().foreach(println)

    sc.stop()
  }

}
