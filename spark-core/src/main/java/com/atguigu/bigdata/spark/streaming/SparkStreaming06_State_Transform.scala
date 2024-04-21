package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lines = ssc.socketTextStream("localhost", 9999)
    // transform方法可以将底层RDD获取到后进行操作
    // 1. DStream功能不完善
    // 2. 需要代码周期性的执行

    // Code：Driver端执行
    lines.transform(
      rdd => {
        // Code：Driver端，（周期性执行）
        rdd.map(
          // Code：Executor端
          str => {
            str
          }
        )
      }
    )

    // Code：Driver端
    lines.map(
      data => {
        // Code：Executor端
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

