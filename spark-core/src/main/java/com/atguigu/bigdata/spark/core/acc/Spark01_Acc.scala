package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-02 17:40
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // reduce：分区内计算，分区间计算，有shuffle阶段
    val i = rdd.reduce(_ + _)
//    println(i)

    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )

    // 打印输出sum = 0
    // 解释原因：rdd.foreach操作是行动算子，Driver端会将Executor端需要的闭包数据拷贝过去，也就是sum会在多个Executor端都保存一份
    // 但是Executor端执行计算结束后，并不会返回计算后的sum结果到Driver端进行聚合，所以Driver端一直保存的都是初始的sum = 0的数据
    // 因此最终Driver端打印的数据就是sum = 0
    println("sum = " + sum)

    sc.stop()
  }

}
