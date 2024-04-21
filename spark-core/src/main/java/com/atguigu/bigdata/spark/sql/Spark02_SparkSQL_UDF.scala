package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Hliang
 * @create 2023-07-20 22:43
 */
object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("preNameFunc", (name:String) => "name-" + name)

    spark.sql("select age,preNameFunc(username) from user").show()



    spark.close()
  }

}
