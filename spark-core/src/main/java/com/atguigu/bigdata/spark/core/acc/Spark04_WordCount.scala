package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Hliang
 * @create 2023-07-02 17:40
 */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello", "spark", "hello"))

    // 累加器实现WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator
    sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加（使用累加器）
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)



    sc.stop()
  }

}

/**
 * 自定义累加器的步骤
 * 1、继承AccumulatorV2，定义泛型
 *    IN：累加器输入数据类型 String
 *    OUT: 累加器输出数据类型mutable.Map[String,Int]
 * 2、重写6个方法
 */
class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]] {

  private val wcMap: mutable.Map[String,Int] = mutable.Map[String,Int]()

  /**
   * 累加器是否为空值，是否是初始状态
   * @return
   */
  override def isZero: Boolean = {
    wcMap.isEmpty
  }

  /**
   * 复制累加器
   * @return
   */
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    new MyAccumulator
  }

  /**
   * 重置累加器
   */
  override def reset(): Unit = {
    wcMap.clear()
  }

  /**
   * 像累加器中添加值
   * @param v
   */
  override def add(v: String): Unit = {
    val newCnt = wcMap.getOrElse(v,0) + 1
    wcMap.update(v,newCnt)
  }

  /**
   * Driver端合并来自不同Executor端累加器中的值
   * @param other
   */
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    val map1 = this.wcMap
    val map2 = other.value
    map2.foreach {
      case (word,cnt) => {
        val newCnt = map1.getOrElse(word,0) + cnt
        map1.update(word,newCnt)
      }
    }
  }

  /**
   * 获取累加器中的值
   * @return
   */
  override def value: mutable.Map[String, Int] = {
    this.wcMap
  }
}
