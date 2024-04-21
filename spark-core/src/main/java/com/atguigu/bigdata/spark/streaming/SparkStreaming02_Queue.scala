package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    val queueDStream = ssc.queueStream(rddQueue, oneAtATime = false)

    val mappedDstream = queueDStream.map((_, 1))

    val reducedDStream = mappedDstream.reduceByKey(_ + _)

    reducedDStream.print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(200)
    }

    ssc.awaitTermination()
  }
}
