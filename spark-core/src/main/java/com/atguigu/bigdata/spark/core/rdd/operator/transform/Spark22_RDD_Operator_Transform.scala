package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark22_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型) leftOuterJoin rightOuterJoin
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a", 5), ("c", 6),("a", 4)
    ))

    // 外连接,主表的数据均会出现,从表的数据会议Option特质的实现类Some或者None的形式存在
    //val leftJoinRDD = rdd1.leftOuterJoin(rdd2)
    val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

    //leftJoinRDD.collect().foreach(println)
    rightJoinRDD.collect().foreach(println)

    sc.stop()
  }

}
