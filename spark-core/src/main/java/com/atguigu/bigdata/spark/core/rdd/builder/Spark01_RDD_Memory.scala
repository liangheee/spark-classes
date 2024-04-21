package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {
    // 1、创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2、执行业务操作
    // 从内存中创建RDD，将内存中集合的数据作为处理的数据源
    val seq: Seq[Int] = Seq(1,5,6,9)
    // parallelize：并行的意思
//    val rdd: RDD[Int] = sc.parallelize(seq)

    // 其实makeRDD底层调用的也是parallelize方法，不过它字面意思更容易理解一些
    val rdd: RDD[Int] = sc.makeRDD(seq)
    rdd.collect().foreach(println)



    // 关闭环境
    sc.stop()




  }

}
