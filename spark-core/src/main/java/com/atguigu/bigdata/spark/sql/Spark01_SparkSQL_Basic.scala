package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author Hliang
 * @create 2023-07-20 17:52
 */
object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 引入转换规则
    import spark.implicits._

    // TODO 执行业务逻辑
    // DataFrame
//    val df: DataFrame = spark.read.json("datas/user.json")
//    df.show()

    // DataFrame => SQL
//    df.createOrReplaceTempView("user")
//    spark.sql("select * from user").show()
//    spark.sql("select age,username from user").show()
//    spark.sql("select avg(age) from user").show()

    // DSL
//    df.select("age","username").show()
//    df.select($"age" + 1).show()
//    df.select('age + 1).show()

    // DataSet
    val seq = Seq(1,2,3,4)
    val ds = seq.toDS()
    ds.show()

    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rdd1: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds1: Dataset[User] = df.as[User]
    val df1: DataFrame = ds1.toDF()

    // RDD <=> DataSet
    val rdd2: RDD[User] = ds1.rdd
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

    // TODO 关闭环境
    spark.close()
  }

  case class User(id: Int,name: String,age: Int)

}
