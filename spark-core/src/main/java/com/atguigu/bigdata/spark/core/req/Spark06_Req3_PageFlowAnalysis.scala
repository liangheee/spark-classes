package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark06_Req3_PageFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)


    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    val actionDataRDD: RDD[UserVisitAction] = actionRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      }
    )

    // TODO 统计从1，2，3，4，5，6，7页面相关的单跳数
    // 1-2 2-3 3-4 4-5 5-6 6-7
    val list = List[Long](1,2,3,4,5,6,7)
    val zipList = list.zip(list.tail)

    // TODO 计算分母
    // TODO 优化，提前过滤不需要的点击页面统计数
    val pageIdMap: Map[Long,Long] = actionDataRDD.filter(
      actionData => {
        // 第7页不用统计到分母中
        list.init.contains(actionData.page_id)
      }
    ).map(
      actionData => {
        (actionData.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap


    // TODO 计算分子
    val groupBySessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    val pageIdUnionToCountRDD: RDD[((Long, Long), Long)] = groupBySessionRDD.mapValues(
      iter => {
        val actions = iter.toList.sortBy(_.action_time)
        val pageIds = actions.map(_.page_id)
        // 1 2 3 4
        // 2 3 4
        // zip操作得到结果 1-2 2-3 3-4
        val pageIdUnions: List[(Long, Long)] = pageIds.zip(pageIds.tail)

        // TODO 进行过滤优化
        pageIdUnions.filter(zipList.contains(_)).map((_, 1L))
      }
    ).map(_._2).flatMap(list => list).reduceByKey(_ + _)

    pageIdUnionToCountRDD.foreach {
      case ((prePageId,lastPageId),sum) => {
        val prePageIdSum = pageIdMap.getOrElse(prePageId, 0L)
        // 注意：整数相除还是整数，所以这里要把sum转为Double
        println(s"页面${prePageId}跳转到页面${lastPageId}的转换率为：${sum.toDouble/prePageIdSum}")
      }
    }

    sc.stop()

  }

}
//用户访问动作表
case class UserVisitAction(
  date: String,//用户点击行为的日期
  user_id: Long,//用户的 ID
  session_id: String,//Session 的 ID
  page_id: Long,//某个页面的 ID
  action_time: String,//动作的时间点
  search_keyword: String,//用户搜索的关键词
  click_category_id: Long,//某一个商品品类的 ID
  click_product_id: Long,//某一个商品的 ID
  order_category_ids: String,//一次订单中所有品类的 ID 集合
  order_product_ids: String,//一次订单中所有商品的 ID 集合
  pay_category_ids: String,//一次支付中所有品类的 ID 集合
  pay_product_ids: String,//一次支付中所有商品的 ID 集合
  city_id: Long
                          )//城市 id

