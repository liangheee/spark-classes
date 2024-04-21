package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Hliang
 * @create 2023-07-21 21:34
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 创建环境对象
    // StreamingContext创建时需要传递两个参数
    // 第一个参数表示环境配置
    // 第二个参数表示批量处理的周期，采集器采集数据的时间（采集周期）
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // TODO 逻辑处理
    // 获取端口数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordCount = wordToOne.reduceByKey(_ + _)

    wordCount.print()

    // 环境对象不能直接关闭，因为关闭了环境对象，也就相当于关闭了采集器，那么程序也就运行结束
    // 当然main方法执行结束后，整个程序也就结束
    // TODO 启动采集器
    ssc.start()

    // TODO 等待采集
    ssc.awaitTermination()

  }

}
