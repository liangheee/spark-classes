package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    ssc.checkpoint("cp")

    // 无状态的操作：每个批次的数据采集后，不会保存状态
    // 有状态的操作：每一批次的数据会采集存储到缓冲区中，并记录状态，通常使用updateStateByKey操作实现
    //              除此以外，有状态的操作还要设置检查点
    val lines = ssc.socketTextStream("localhost", 9999)
    val mappedDStream = lines.map((_, 1))
    // 第一个参数seq：表示相同key的value的集合
    // 第二个参数buffer：表示缓冲区中相同key的value集合
    val state: DStream[(String, Int)] = mappedDStream.updateStateByKey(
      (seq: Seq[Int], buffer: Option[Int]) => {
        val newCnt = buffer.getOrElse(0) + seq.sum
        Option(newCnt)
      }
    )

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

