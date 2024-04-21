package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark02_RDD_File_Par2 {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 分区分配的案例
    // 下面一共14个字节
    // totalSize = 14
    // goalSize = 14 / 2 2个分区，每个分区7个字节
    // 1234567@@ => 012345678
    // 89@@ => 9101112
    // 0 => 13
    // 第0个分区 => [0,7] 1234567@@
    // 第1个分区 => [7,14] 89@@0
    val rdd = sc.textFile("datas/word.txt", 2)

    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()




  }

}
