package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Hliang
 * @create 2023-07-21 12:45
 */
object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","liangheee")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop102:8020/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._




    spark.close()
  }

}
