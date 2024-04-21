package com.atguigu.bigdata.spark.streaming

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming10_MockData {
  def main(args: Array[String]): Unit = {

    // 创建kafka配置
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    // 连接kafka集群，生产数据
    val kafkaProducer = new KafkaProducer[String, String](properties)

    while(true){
      val datas: ListBuffer[String] = mockData()
      for (elem <- datas) {
        val record = new ProducerRecord[String, String]("atguiguNew", "", elem)
        kafkaProducer.send(record)
      }
      Thread.sleep(500)
    }

    kafkaProducer.close()

  }

  /**
   * 模拟生成实时数据
   * 格式：timestamp area city userid adid
   *       时间戳    区域  城市  用户id 广告id
   */
  def mockData(): ListBuffer[String] ={

    var list = ListBuffer[String]()

    val areas = List("华东", "华北", "华南", "华西")
    val cities = List("南京","北京","深圳","成都")

    for(i <- 1 to new Random().nextInt(31)){
      val timestamp = System.currentTimeMillis()
      val random = new Random().nextInt(4)
      val area = areas(random)
      val city = cities(random)
      val userid = new Random().nextInt(6) + 1
      val adid = new Random().nextInt(6) + 1

      // 拼接实时数据
      val data = timestamp + " " + area + " " + city + " " + userid + " " + adid
      list += data
    }
    list
  }
}

