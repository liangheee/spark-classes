package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark02_RDD_File_Par1 {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // TODO 通过文件创建RDD的分区分配原则
    // FileInputformat是针对每个文件进行单独读入的，这是一个大前提，所以如果读入的是文件夹，我们要对其中的每一个文件进行单独的计算
    // 1、数据是以行的形式进行读取
    //    spark读取文件时，采用的是hadoop的方式进行读取，所以一行一行读取，和字节数没有关系
    // 2、数据读取时以偏移量为单位，偏移量不会被重复读取
    // 1@@ =》 012
    // 2@@ =》 345
    // 3 =》 6
    // 数据分区的偏移量范围的计算（闭区间）
    // 第0个分区 => [0, 3] => 1@@2@@
    // 第1个分区 => [3, 6] => 3
    // 第2个分区 => [6, 7] =>
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()




  }

}
