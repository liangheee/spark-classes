package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark06_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    // groupBy: 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同key值的数据h会放置在一个组中
    val rdd = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"),2)

    // 数据根据指定的规则进行分组，分区数是默认不变的，但是数据会被重新打乱组合
    // 我们通常将这种重新打乱组合的操作为shuffle
    // 极限情况下，数据可能被分在同一个分区中
    // 注意：分组和分区没有必然的关系！分区和分组并不是一对一的关系，同一分区中可能会有多个分组
    val groupRDD = rdd.groupBy(_.charAt(0))
    groupRDD.saveAsTextFile("output1")

    sc.stop()
  }

}
