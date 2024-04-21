package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - sample
    // sample：根据指定的规则从数据源中抽取数据
    // 参数解读：
    //  withReplacement：每次抽取的数据是否放回
    //  fraction：如果抽取的数据不放回，该参数表示数据抽取的基准值，随机算法获取的随机值小于该参数值则保留，随机算法获取的随机值大于该参数值的则不丢弃（0：全不取，1：全取）
    //            如果抽取的数据放回，那么该参数表示数据源中每一个数据期望被抽到的次数（具体抽到的次数不一定就是这个，可能大于，也可能小于这个值）
    //  seed：抽取数据时生成随机值的随机算法的种子，如果该值被显式指定，那么每一次运行生成的随机值就是确定的
    //        如果种子没有被指定，那么底层默认采用的是系统的时钟
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    val sampleRDD: RDD[Int] = rdd.sample(true, 0.4, 1)

    sampleRDD.collect().foreach(println)

    sc.stop()
  }

}
