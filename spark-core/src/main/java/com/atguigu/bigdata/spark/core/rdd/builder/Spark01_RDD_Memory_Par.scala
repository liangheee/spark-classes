package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // RDD的并行度&分区
    // makeRDD方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数可以不传递，那么makeRDD方法会使用默认值：defaultParallelism（默认的并行度）
    // defaultParallelism是一个方法，其底层调用链路如下
    // defaultParallelism =》
    //     taskScheduler.defaultParallelism =》
    //          backend.defaultParallelism() =》
    //                scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 故最终其实是使用spark.default.parallelism参数进行配置，并且实在SparkConf中进行此项配置
    // scheduler.conf其实就是SparkConf
    // 所以默认情况下，spark从配置对象SparkConf中获取配置参数soark.default.parallelism
    // 如果获取不到，那么使用totalCores属性，这个属性的取值为当前运行环境下的最大可用cpu核数
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()




  }

}
