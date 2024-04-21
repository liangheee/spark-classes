package com.atguigu.bigdata.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark03_Req1_HotCategoryTop10Analysis2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)
    // 需求1：Top10热门品类
    // 本项目的需求优化为：
    // 先按照点击数排名，点击数高的靠前，如果点击数相同，则比较订单数，如果订单数相同，则比较支付数
    // 需求1的需求分析：
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // TODO 优化方案二
    // reduceByKey聚合操作较多，会导致大量的shuffle，影响性能
    // 注意，类似如reduceByKey这样的聚合操作，spark底层都会对其进行优化，比如自动对数据源进行缓存cache操作等
    // 优化的具体方案是
    // 直接最开始对actionRDD进行处理，直接获取到（品类id，（点击数，0，0）） （品类id，（0，订单数，0）） （品类id，（0，0，支付数））

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

    val result = analysisRDD.sortBy(_._2, false).take(10)
    result.foreach(println)

    sc.stop()

  }

}
