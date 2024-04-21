package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - coalesce
    // coalesce：默认情况下不会将分区的数据打乱重新组合 (默认shuffle参数为false)
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要数据均衡，可以进行shuffle处理
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

//    val coalesceRDD: RDD[Int] = rdd.coalesce(2)
//    val coalesceRDD: RDD[Int] = rdd.coalesce(2,true)

    // coalesce也可以增加分区，不过此时必须进行shuffle
    val coalesceRDD: RDD[Int] = rdd.coalesce(3,true)
    coalesceRDD.saveAsTextFile("output")

    sc.stop()
  }

}
