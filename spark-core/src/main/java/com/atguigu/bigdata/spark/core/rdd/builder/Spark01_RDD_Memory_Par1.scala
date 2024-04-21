package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark01_RDD_Memory_Par1 {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // TODO makeRDD的分区分配原则
    // ParallelCollectionRDD.slice() => position方法中结果是(0,1)  (1，3)  (3,5)
    // [1] [2,3] [4,5]
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5), 3)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()




  }

}
