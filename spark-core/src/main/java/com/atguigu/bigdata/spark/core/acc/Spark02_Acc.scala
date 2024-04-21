package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-07-02 17:40
 */
object Spark02_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Accumulator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认就提供了简单的数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

//    sc.doubleAccumulator()
//      sc.collectionAccumulator()

    rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )

    // 累加器复制拷贝到Executor端计算完成后，Executor端会将累加器返回给Driver端进行聚合
    // 所以最终打印结果就是sum = 10
    println(sumAcc.value)


    sc.stop()
  }

}
