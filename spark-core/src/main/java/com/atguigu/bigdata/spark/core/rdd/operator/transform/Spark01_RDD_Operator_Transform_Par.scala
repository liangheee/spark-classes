package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 14:14
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - map
    // 1、rdd的计算逻辑是，同一分区内的数据是一个执行完其涉及的所有逻辑再进行下一个的计算
    //    只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据
    //    分区内数据的执行是有序的
    // 2、不同分区数据计算是无序的（这个就是并行计算的机制）
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD = rdd.map(
      num => {
        println(">>>>>>>> " + num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("######" + num)
        num
      }
    )

    mapRDD1.collect()

    sc.stop()

  }

}
