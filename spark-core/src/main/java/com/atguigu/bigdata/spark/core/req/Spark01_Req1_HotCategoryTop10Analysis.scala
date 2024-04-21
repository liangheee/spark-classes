package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)
    // 需求1：Top10热门品类
    // 本项目的需求优化为：
    // 先按照点击数排名，点击数高的靠前，如果点击数相同，则比较订单数，如果订单数相同，则比较支付数
    // 需求1的需求分析：
    val actionRDD = sc.textFile("datas/user_visit_action.txt")
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

    // 4、根据（品类id，点击数）（品类id，订单数）（品类id，支付数）三个元组的value进行排序比较，从而得到结果
    //  但是这样做较为复杂，每次需要看上次比较的结果是否为相同，相同情况下需要比较下一个指标
    //  所以我们进行优化，看着是否能够把（品类id，点击数）（品类id，订单数）（品类id，支付数）优化成为一个整体
    //  （品类id，（点击数，订单数，支付数）），从而利用这个tuple进行比较，获取热门品类Top10
    //  通过连接的方案将三个单独的tuple连接成为最终的一个tuple
    //  一共有四种选择方案，join、leftOuterJoin、zip、cogroup
    //   join：这种方案不能选择，因为join相当于MySQL中的等值连接，三个单独的tuple中可能品类id不一定完全相同，最终就会丢数据
    //   leftOuterJoin：左外连接，主表中的品类id，在从表中并不一定存在
    //   zip：zip拉链的方式对对应的分区的数据有很严格要求，同时对分区数也具有严格的要求
    //   cogroup：connect + group 会先进行分组，然后再进行连接操作，这个算子相当于把参与计算的所有表都当作主表，是同一级别的，不再有从表的说法

    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickCountRDD.cogroup(orderCountRDD, payCountAction)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIterator, orderIterator, payIterator) => {
        val clickIter = clickIterator.iterator
        var clickCnt = 0
        if (clickIter.hasNext) {
          clickCnt = clickIter.next()
        }

        val orderIter = orderIterator.iterator
        var orderCnt = 0
        if (orderIter.hasNext) {
          orderCnt = orderIter.next()
        }

        val payIter = payIterator.iterator
        var payCnt = 0
        if (payIter.hasNext) {
          payCnt = payIter.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }
    val result = analysisRDD.sortBy(_._2, false).take(10)
    result.foreach(println)

    sc.stop()
  }

}
