package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, functions}

/**
 * @author Hliang
 * @create 2023-07-20 22:43
 */
object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("myAgeAvg", new MyAgeAvg)

    spark.sql("select myAgeAvg(age) from user").show()



    spark.close()
  }

  /**
   * 实现自定义弱类型的UDAF函数
   * 1、继承UserDefinedAggregateFunction
   * 2、重写方法（8个）
   */
  class MyAgeAvg extends UserDefinedAggregateFunction {
    // 输入的数据格式
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("age",LongType)
      ))
    }

    // 缓冲区数据格式
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("ageSum",LongType),
        StructField("cnt",LongType)
      ))
    }

    // 输出数据类型
    override def dataType: DataType = {
      LongType
    }

    // 函数输出是否稳定（也就是相同的输入会不会得到相同的输出）
    override def deterministic: Boolean = true

    // 初始化缓冲区
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0,0L)
      buffer.update(1,0L)
    }

    // 数据流入缓冲区的计算
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0,buffer.getLong(0) + input.getLong(0))
      buffer.update(1,buffer.getLong(1) + 1)
    }

    // spark是分布式计算，所以缓冲区可能有多个，需要合并多个缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0,buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1,buffer1.getLong(1) + buffer2.getLong(1))

    }

    // 计算缓冲区输出
    override def evaluate(buffer: Row): Any = {
        buffer.getLong(0) / buffer.getLong(1)
    }

  }

}
