package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:27
 */
object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persists")

    // TODO 多种缓存方式的比较与最佳实践建议
    // cache：将数据临时存储在内存文件中，从而实现数据重用
    // persist：将数据临时存储在磁盘文件中进行数据重用
    //          涉及到磁盘的IO，性能较低，但是数据安全
    //          如果作业执行完毕，临时保存的数据文件将会丢失
    // checkpoint：将数据长久的保存在磁盘文件中进行数据重用
    //            涉及到磁盘IO，性能较低，但是数据安全
    //            为了保证数据的安全，所以一般情况下，会独立运行一次作业
    //            为了能够提高效率，一般情况下，是需要和cache联合使用的  TODO 这就是RDD持久化的最佳实践


    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala,Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatMapRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word,1)
    })

    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)

    println("************************************")

    val groupByKeyRDD = mapRDD.groupByKey()

    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
