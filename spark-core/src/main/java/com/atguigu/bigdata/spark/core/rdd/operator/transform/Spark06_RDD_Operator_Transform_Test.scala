package com.atguigu.bigdata.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark06_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - groupBy
    // 从服务器日志数据apache.log中获取每个时间段访问量
    val rdd = sc.textFile("datas/apache.log")

    val mapRDD: RDD[(String,Int)] = rdd.map(
      lines => {
        val datas = lines.split(" ")
        val date = datas(3)
        val sdf1 = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val myDate: Date = sdf1.parse(date)
        val sdf2 = new SimpleDateFormat("HH")
        val hours = sdf2.format(myDate)
        (hours, 1)
      }
    )

    val groupByRDD = mapRDD.groupBy(_._1)

    val resultRDD: RDD[(String,Int)] = groupByRDD.map(
      tuple => {
        (tuple._1, tuple._2.size)
      }
    )

    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
