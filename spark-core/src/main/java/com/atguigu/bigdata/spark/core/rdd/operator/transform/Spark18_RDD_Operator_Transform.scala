package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark18_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型) aggregateByKey
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ),2)


    // aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
//    rdd.aggregateByKey("")(_ + _ , _ + _).collect().foreach(println)
    // 所以初始值为字符串，所以最终输出的也就是字符串

    // 获取相同key的数据的平均值 => ("a",3) ("b",4)

    val aggRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t1, x) => (t1._1 + x, t1._2 + 1),
      (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2)
    )

    val mapRDD = aggRDD.map {
      case (key, (nums, cnt)) => (key, nums / cnt)
    }

    mapRDD.collect().foreach(println)

    sc.stop()
  }

}
