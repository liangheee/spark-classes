package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型) aggregateByKey
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ),2)
    // (a,【1,2】), (a, 【3，4】)
    // (a, 2), (a, 4)
    // (a, 6)

    // aggregateByKey：存在函数柯里化，有两个参数列表
    // 第一个参数列表需要传递一个参数，表示为初始值
    // 第二个参数列表需要传递两个参数
    //          第一个参数表示分区内计算规则
    //          第二个参数表示分区间计算规则
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x + y
    ).collect().foreach(println)


    sc.stop()
  }

}
