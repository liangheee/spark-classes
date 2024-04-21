package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 统计出每一个省份每个广告被点击数量排行的Top3
    // agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

    // 1、读取数据源agent.log
    val datasRDD = sc.textFile("datas/agent.log")

    // 2、将数据转换为((省份，广告), 1)
    val mapRDD = datasRDD.map(
      line => {
        val datas = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    // 3、根据(省份,广告)作为key进行分组聚合得到((省份，广告),sum)
    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    // 4、将分组聚合结果进行转换成为(省份,(广告,sum))
    val newMapRDD = reduceByKeyRDD.map {
      case ((province, advertisement), sum) => (province, (advertisement, sum))
    }

    // 5、对(省份,(广告,sum))进行按照key的分组，获得(省份,((广告A,sumA),(广告B,sumB)))
    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    // 6、(省份,((广告A,sumA),(广告B,sumB)))进行组内排序，并取点击量的Top3
    val resultRDD = groupByKeyRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering[Int].reverse).take(3)
      }
    )

    // 7、对结果进行收集操作，并打印输出到控制台
    resultRDD.collect().foreach(println)

    sc.stop()
  }

}
