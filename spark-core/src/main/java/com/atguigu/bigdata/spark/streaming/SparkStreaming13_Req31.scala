package com.atguigu.bigdata.spark.streaming

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * @author Hliang
 * @create 2023-07-22 16:54
 */
object SparkStreaming13_Req31 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("cp")

    // 创建kafka配置信息
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "Requirement",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->"org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 从kafka采集到实时数据
    val inputDS: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(
      ssc,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe(Seq("atguiguNew"), kafkaParams)
    )

    val kafkaDStream: DStream[String] = inputDS.map(_.value())

    val adClickData = kafkaDStream.map(
      data => {
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    val statisticData: DStream[(Long, Int)] = adClickData.map(
      data => {
        // 13秒 =》10秒
        // 19秒 =》10秒
        // 25秒 =》 20秒
        // 37秒 =》30秒
        // 。。。。
        // 只取秒的十位数
        val dt = data.dt.toLong / 10000 * 10000
        (dt, 1)
      }
    ).reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y,
      Seconds(60),
      Seconds(10)
    )

    statisticData.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()
        // 因为reduceByKey涉及shuffle，可能会把数据打乱，所以我们对rdd进行排序
        val datas: Array[(Long, Int)] = rdd.sortBy(_._1, true).collect()
        val sdf = new SimpleDateFormat("mm:ss")
        datas.foreach{
          case (dt,cnt) => {
            val dateStr = sdf.format(new Date(dt))
            list.append(
              s"""
                 | {"xtime":"${dateStr}","yval":"${cnt}"}
                 |""".stripMargin)
          }
        }

        // 输出文件到adclick.json中
        val writer = new PrintWriter(new File("E:\\idea_project\\spark-classes\\datas\\adclick\\adclick.json"))
        writer.println("[" + list.mkString(",") + "]")
        writer.flush()
        writer.close()
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
  case class AdClickData(dt: String, area: String, city: String, userid: String, adid: String)
}

