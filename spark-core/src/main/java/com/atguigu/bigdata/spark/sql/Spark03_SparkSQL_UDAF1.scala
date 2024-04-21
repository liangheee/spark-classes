package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

/**
 * @author Hliang
 * @create 2023-07-20 22:43
 */
object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("myAgeAvg",functions.udaf(new MyAgeAvg))

    spark.sql("select myAgeAvg(age) from user").show()

    spark.close()
  }

  /**
   * 实现自定义弱类型的UDAF函数
   * 1、继承Aggregator
   *    IN：输入数据类型
   *    BUF：缓冲区数据类型
   *    OUT：输出数据类型
   * 2、重写方法（8个）
   */
  case class Buf(var age: Long,var cnt: Long)
  class MyAgeAvg extends Aggregator[Long,Buf,Long] {
    // 初始化缓冲区
    override def zero: Buf = {
      Buf(0L,0L)
    }

    // 数据流向缓冲区的聚合
    override def reduce(buffer: Buf, input: Long): Buf = {
      buffer.age = buffer.age + input
      buffer.cnt = buffer.cnt + 1L
      buffer
    }

    // 合并多个缓冲区
    override def merge(buffer1: Buf, buffer2: Buf): Buf = {
      buffer1.age = buffer1.age + buffer2.age
      buffer1.cnt = buffer1.cnt + buffer2.cnt
      buffer1
    }

    // 完成最终的计算
    override def finish(buffer: Buf): Long = {
      buffer.age / buffer.cnt
    }

    // 缓冲区编码：自定义的类型，编码固定为Encoders.product，其它系统类型则是Encoders.scalaXXX
    override def bufferEncoder: Encoder[Buf] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
