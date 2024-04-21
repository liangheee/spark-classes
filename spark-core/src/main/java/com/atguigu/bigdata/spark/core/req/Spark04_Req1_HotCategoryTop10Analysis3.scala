package com.atguigu.bigdata.spark.core.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Hliang
 * @create 2023-07-04 13:32
 */
object Spark04_Req1_HotCategoryTop10Analysis3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)
    // 需求1：Top10热门品类
    // 本项目的需求优化为：
    // 先按照点击数排名，点击数高的靠前，如果点击数相同，则比较订单数，如果订单数相同，则比较支付数
    // 需求1的需求分析：
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // TODO 优化方案三：使用累加器进行处理，从而完全避免shuffle
    // 创建累加器，并且进行注册
    val hotCategoryAccumulator = new HotCategoryAccumulator
    sc.register(hotCategoryAccumulator,"hotCategoryAccumulator")

    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if(datas(6) != "-1"){
          hotCategoryAccumulator.add((datas(6),"click"))
        }else if(datas(8) != "null"){
          val cids = datas(8).split(",")
          cids.foreach(
            cid => hotCategoryAccumulator.add((cid,"order"))
          )
        }else if(datas(10) != "null"){
          val cids = datas(10).split(",")
          cids.foreach(
            cid => hotCategoryAccumulator.add((cid,"pay"))
          )
        }
      }
    )

    val hcMap: mutable.Map[String, HotCategory] = hotCategoryAccumulator.value
    val analysisReulst: mutable.Iterable[(String, Int, Int, Int)] = hcMap.map {
      case (cid, hotCategory) => (cid, hotCategory.clickCnt, hotCategory.orderCnt, hotCategory.payCnt)
    }

    val result: List[(String, Int, Int, Int)] = analysisReulst.toList.sortWith(
      (left, right) => {
        if (left._2 > right._2) {
          true
        } else if (left._2 == right._2) {
          if (left._3 > right._3) {
            true
          } else if (left._3 == right._3) {
            left._4 > right._4
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

    result.foreach(println)

    sc.stop()

  }

}

case class HotCategory(cid: String,var clickCnt: Int,var orderCnt: Int,var payCnt: Int)

/**
 * 自定义累加器
 * 1、继承AccumulatorV2
 * IN：（品类id，类型）  类型的意思是：点击操作 | 订单操作 | 支付操作
 * OUT：mutable.Map[String,HotCategory]
 * 2、重写6个方法
 */
class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,HotCategory]] {

  val hcMap: mutable.Map[String,HotCategory] = mutable.Map[String,HotCategory]()

  override def isZero: Boolean = {
    hcMap.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new HotCategoryAccumulator
  }

  override def reset(): Unit = {
    hcMap.clear()
  }

  override def add(v: (String, String)): Unit = {
    val cid = v._1
    val operator = v._2
    val category = hcMap.getOrElse(cid,HotCategory(cid, 0, 0, 0))
    if(operator == "click"){
      category.clickCnt += 1
    }else if(operator == "order"){
      category.orderCnt += 1
    }else if(operator == "pay"){
      category.payCnt += 1
    }
    hcMap.update(cid,category)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    val map1 = this.hcMap
    val map2 = other.value

    map2.foreach {
      case (cid,hotCategory) => {
          val category = map1.getOrElse(cid, HotCategory(cid,0, 0, 0))
          category.clickCnt += hotCategory.clickCnt
          category.orderCnt += hotCategory.orderCnt
          category.payCnt += hotCategory.payCnt
          map1.update(cid,category)
      }
    }
  }

  override def value: mutable.Map[String, HotCategory] = {
    this.hcMap
  }
}
