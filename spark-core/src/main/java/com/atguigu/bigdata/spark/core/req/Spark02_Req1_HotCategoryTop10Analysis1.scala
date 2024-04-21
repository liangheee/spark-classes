package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark02_Req1_HotCategoryTop10Analysis1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)
    // 需求1：Top10热门品类
    // 本项目的需求优化为：
    // 先按照点击数排名，点击数高的靠前，如果点击数相同，则比较订单数，如果订单数相同，则比较支付数
    // 需求1的需求分析：
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // TODO 优化方案一
    // Question1：actionRDD被重复使用  ===》 解决方案：缓存数据源
    actionRDD.cache()

    // Question2：cogroup有可能会导致shuffle ===》 解决方案：不适用cogroup，用其它方案实现
    //      将聚合结果（品类id，点击数）、（品类id，下单数）、（品类id，支付数）
    //      转换为 （品类id，（点击数，0，0）） （品类id，（0，下单数，0））  （品类id，（0，0，支付数））
    //      然后对所有的结果进行union聚合形成一个数据源，再进行后续的WordCount操作即可


    // 1、获取（品类id，点击数）
    // 先进行过滤，从而减少数据量，加快spark计算
    val clickactionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )

    // 进行统计获取（品类id，点击数）的WordCount
    val clickCountRDD = clickactionRDD.map(
      clickAction => {
        val datas = clickAction.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 转换为（品类id，（点击数，0，0））
    val clickCountMapRDD = clickCountRDD.map {
      case (cid, clickCnt) => (cid, (clickCnt, 0, 0))
    }

    // 2、获取（品类id，订单数）
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD = orderActionRDD.flatMap(
      orderAction => {
        val datas = orderAction.split("_")
        val cid = datas(8).split(",")
        cid.map((_,1))
      }
    ).reduceByKey(_ + _)

    // 将（品类id，订单数）转换为（品类id，（0，订单数，0））
    val orderCountMapRDD = orderCountRDD.map {
      case (cid, orderCnt) => (cid, (0, orderCnt, 0))
    }

    // 3、获取（品类id，支付数）
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountAction = payActionRDD.flatMap(
      payAction => {
        val datas = payAction.split("_")
        val cid = datas(10).split(",")
        cid.map((_,1))
      }
    ).reduceByKey(_ + _)

    // 将（品类id，支付数）转换为（品类id，（0，0，支付数））
    val payCountMapRDD = payCountAction.map {
      case (cid, payCnt) => (cid, (0, 0, payCnt))
    }

    // 4、Union联合数据源后进行排序获取Top10
    val unionRDD: RDD[(String, (Int, Int, Int))] = clickCountMapRDD.union(orderCountMapRDD).union(payCountMapRDD)
    val analysisRDD = unionRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val result = analysisRDD.sortBy(_._2, false).take(10)
    result.foreach(println)

    sc.stop()

  }

}
