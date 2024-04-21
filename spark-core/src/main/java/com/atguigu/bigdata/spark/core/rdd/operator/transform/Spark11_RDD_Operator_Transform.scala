package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - repartition
    // coalesce：该算子也可以扩大分区，但是如果不进行shuffle操作，是没有意义的，此时扩大分区不起作用
    // 所以如果想要实现扩大分区的效果，必须进行shuffle操作
    // spark提供了一个简化的操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition，底层代码其实就是coalesce，而且参数shuffle一定为true
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

    // coalesce也可以增加分区，不过此时必须进行shuffle
//    val coalesceRDD: RDD[Int] = rdd.coalesce(3,true)
//    coalesceRDD.saveAsTextFile("output")
    val repartitionRDD: RDD[Int] = rdd.repartition(3)
    repartitionRDD.saveAsTextFile("output")

    sc.stop()
  }

}
