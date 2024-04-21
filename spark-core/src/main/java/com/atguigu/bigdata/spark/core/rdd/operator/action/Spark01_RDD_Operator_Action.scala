package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-28 17:11
 */
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // TODO - 行动算子
    // 所谓行动算子,其实就是戳发作业(job)执行的方法
    // 底层调用的其实是环境对象SparkContext的runJob方法
    // 底层代码中会通过DAGScheduler类下的handleJobSubmitted方法中创建ActiveJob,并提交执行
    rdd.collect()

    sc.stop()
  }

}
