package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark02_RDD_File_Par {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // textFile可以将文件作为数据源，默认也可以设置分区
    // 其默认分区数是minPartitions，底层调用的是defaultMinPartitions方法  =》 math.min(defaultParallelism, 2)
//    val rdd = sc.textFile("datas/1.txt")
    // 如果不想使用默认分区数，可以通过第二个参数指定分区数
    // Spark通过读取文件的方式创建RDD，底层其实使用的是Hadoop的方式读取文件
    // 我们设置的分区数并一定就是最终运行结果的分区数，这必须追踪hadoop底层源码了
    // 分区数的计算方式
    // totalSize = 7bytes
    // goalSize = 7bytes / 2  结果是 3bytes余1bytes
    // 根据FIleInputFormat的规则，剩余的大小要大于切片大小的1.1倍，也就是4/3 > 1.1
    // 所以分成三片，3 3 1
    val rdd = sc.textFile("datas/1.txt", 2)

    rdd.saveAsTextFile("output")


    // TODO 关闭环境
    sc.stop()




  }

}
