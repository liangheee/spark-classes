package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-01 10:54
 */
object Spark01_RDD_Part {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxxxxxx"),
      ("cba", "xxxxxxxxx"),
      ("wnba", "xxxxxxxxx"),
      ("nba", "xxxxxxxxx"),
    ),3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy( new MyPartitioner )

    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /**
   * 自定义分区器
   * 1.继承Partitioner
   * 2.重写numPartitions和getPartition方法
   * TODO 注意的是：分区器是对key-value类型数据使用的  非key-value类型的RDD分区的值是None
   */
  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3  // 当然我们可以通过构造器传入

    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }
}
