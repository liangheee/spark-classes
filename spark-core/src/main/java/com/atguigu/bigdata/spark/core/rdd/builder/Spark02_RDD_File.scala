package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-26 17:51
 */
object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {
    // 1、创建环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 2、执行业务操作
    // 从外部存储中创建RDD，比如从文件中创建RDD，文件中的数据作为创建RDD的数据源
    // path是以当前项目的根路径为基准的，可以是填写绝对路径，也可以填写相对路径
//    val rdd: RDD[String] = sc.textFile("E:\\idea_project\\spark-classes\\datas\\1.txt")
//    val rdd = sc.textFile("datas/1.txt")
    // 可以读取整个文件夹下面的所有内容
//    val rdd = sc.textFile("datas")

    // path中配置通配符，按照通配符进行读取
    val rdd = sc.textFile("datas/1*.txt")

    rdd.collect().foreach(println)

    // 关闭环境
    sc.stop()




  }

}
