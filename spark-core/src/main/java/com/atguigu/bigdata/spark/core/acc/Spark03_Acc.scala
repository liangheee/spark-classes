package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-02 17:40
 */
object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单的数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

//    sc.doubleAccumulator()
//      sc.collectionAccumulator()

    val mapRDD: RDD[Int] = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    // 多加：转换算子中调用累加器，如果多次调用行动算子的话，那么就会产生多加问题
    // 一般情况下，累加器会放置在行动算子中进行操作
    println(sumAcc.value)


    sc.stop()
  }

}
