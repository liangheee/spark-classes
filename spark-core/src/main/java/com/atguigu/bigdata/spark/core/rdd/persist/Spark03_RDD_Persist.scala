package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-30 21:27
 */
object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Persists")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala,Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatMapRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatMapRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word,1)
    })

    // 进行数据的缓存
    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘文件，需要更改存储级别
    // cache底层其实就是调用的persist方法，存储级别是MEMORY_ONLY
    mapRDD.cache()

    // 持久化操作只有在行动算子执行时完成 ==> 默认是存储到系统的临时文件，执行结束后会删除临时文件
//    mapRDD.persist(StorageLevel.DISK_ONLY)


    val reduceByKeyRDD = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.collect().foreach(println)

    println("************************************")

    val groupByKeyRDD = mapRDD.groupByKey()

    groupByKeyRDD.collect().foreach(println)

    sc.stop()
  }

}
