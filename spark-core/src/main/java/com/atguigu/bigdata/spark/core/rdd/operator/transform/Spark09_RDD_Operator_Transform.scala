package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - distinct
    // distinct：去重
    // scala语法中的distinct去重底层采用的是HashSet的方式进行去重
    // 但是spark中的算子distinct底层是通过一系列算子封装进行的去重
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    // 故对1,2,3,4,4,3,2,1去重流程如下
    // (1,null) (1,null) (2,null) (2,null) (3,null) (3,null) (4,null) (4,null)
    // (1,null) (2,null) (3,null) (4,null)
    // 1 2 3 4
    val rdd = sc.makeRDD(List(1,2,3,4,4,3,2,1),2)

    val distinctRDD: RDD[Int] = rdd.distinct()

    distinctRDD.collect().foreach(println)

    sc.stop()
  }

}
