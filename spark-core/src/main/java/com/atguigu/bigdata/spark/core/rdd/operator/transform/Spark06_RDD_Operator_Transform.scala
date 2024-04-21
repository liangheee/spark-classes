package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark06_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    // groupBy: 会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同key值的数据h会放置在一个组中
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    def groupByFunction(num: Int): Int = {
      num
    }

    val groupByRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupByFunction)
    groupByRDD.collect().foreach(println)

    sc.stop()
  }

}
