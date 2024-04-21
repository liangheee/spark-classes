package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 1、配置kafka的参数
    val kafkaParams : Map[String,Object] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "Kafka-SparkStreaming",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 2、消费kafka数据创建DStream
    // TODO 采用kafkaUtils工具，使用DirectAPI
    // createDirectStream参数解读
    // 第一个参数：SparkStreaming环境对象StreamingContext
    // 第二个参数：采集器节点和计算节点位置选择策略，通常采用PreferConsistent策略，将节点选择交给Spark自身决定
    // 第三个参数：消费策略，比如订阅哪个主题，并且配置kafka参数
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe(Set("atguiguNew"), kafkaParams)
    )

    // 3、我们通常只关心消息的value
    val valueDStream = kafkaDStream.map(_.value())
    valueDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

