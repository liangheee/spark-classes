package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // TODO 自定义个采集器
    val messageDS = ssc.receiverStream(new MyReceiver)
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
/**
 * 自定义一个采集器
 * 可以用来采集多种数据源的数据
 * 1、继承Receiver，定义泛型
 * 2、重写onStart和onStop方法
 */
class MyReceiver extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY) {

  private var flag = true

  override def onStart(): Unit = {
    // TODO 1、连接数据源
    // TODO 2、创建新的线程去采集数据
    new Thread(new Runnable {
      override def run(): Unit = {
        while(flag) {
          val message = "采集的消息-" + Random.nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }
    }).start()

  }

  override def onStop(): Unit = {
    flag = false
  }
}

