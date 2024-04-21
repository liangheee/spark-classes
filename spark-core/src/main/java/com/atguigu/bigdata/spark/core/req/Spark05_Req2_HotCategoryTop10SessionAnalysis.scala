package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // TODO 需求2：Top10热门品类中每个品类的Top10活跃session统计
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val top10CategoryIds: Array[String] = getCategoryTop10(actionRDD)

    // 过滤原始数据中的点击数据，并且包含在Top10品类中
    val filterActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          top10CategoryIds.contains(datas(6))
        } else {
          false
        }
      }
    )

    // 对数据进行转换，后进行聚合，从而得到（（品类id，session），sum）
    val reduceByKeyRDD = filterActionRDD.map(
      action => {
        val datas = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 将（（品类id，session），sum）转换为（品类id，（session，sum））
    val mapRDD = reduceByKeyRDD.map {
      case ((cid, session), sum) => (cid, (session, sum))
    }
    // 对（品类id，（session，sum））进行按照key的聚合
    val groupByKeyRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    // 将分组后的数据进行点击统计量排名，取前10名
    val newMapRDD: RDD[(String, List[(String, Int)])] = groupByKeyRDD.mapValues(
      iter => {
        iter.toList.sortBy(tuple => tuple._2)(Ordering[Int].reverse).take(10)
      }
    )

    newMapRDD.collect().foreach(println)


    sc.stop()

  }

  def getCategoryTop10(actionRDD: RDD[String]) ={
    val flatMapRDD = actionRDD.flatMap(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val cid = datas(8).split(",")
          cid.map((_,(0,1,0)))
        } else if (datas(10) != "null") {
          val cid = datas(10).split(",")
          cid.map((_,(0,0,1)))
        } else {
          Nil
        }
      }
    )

    val analysisRDD = flatMapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }

}
