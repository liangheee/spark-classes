package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:27
 */
object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persists")

    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala,Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatMapRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word,1)
    })

    // checkpoint 检查点数据缓存
    // checkpoint 需要落盘，需要指定检查点保存路径
    // 检查点路径保存的文件，当作业执行结束后，不会被删除
    // 一般保存路径都是在分布式存储系统，比如Hadoop的HDFS中
    // TODO 需要注意的是，checkpoint算子会在底层单独跑一次job（RunJob）
    // TODO 相当于在此次运行过程中，该缓存任务会执行两次
    mapRDD.checkpoint()

    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)

    println("************************************")

    val groupByKeyRDD = mapRDD.groupByKey()

    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
