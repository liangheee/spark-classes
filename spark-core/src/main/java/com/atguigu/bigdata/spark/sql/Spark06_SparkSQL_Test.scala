package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Hliang
 * @create 2023-07-21 12:45
 */
object Spark06_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","liangheee")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1、引入相关pom依赖，包括mysql驱动和hive-exec
    // 2、在classpath下加入hive-site.xml文件，将hive托管给spark
    // 3、spark构建时启动Hive支持 enableHiveSupport()

    spark.sql("show tables").show()




    spark.close()
  }

}
