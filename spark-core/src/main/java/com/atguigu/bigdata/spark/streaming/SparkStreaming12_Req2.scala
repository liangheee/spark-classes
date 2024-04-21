package com.atguigu.bigdata.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.util.JDBCUtil
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
object SparkStreaming12_Req2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

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

    val mappedDS = kafkaDStream.map(
      data => {
        val datas = data.split(" ")
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dateStr = sdf.format(datas(0).toLong)
        ((dateStr, datas(1), datas(2), datas(4)), 1)
      }
    ).reduceByKey(_ + _)

    mappedDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val ps = conn.prepareStatement(
              """
                | insert into area_city_ad_count (dt,area,city,adid,count)
                | values (?,?,?,?,?)
                | on duplicate key
                | update count = count + ?
                |""".stripMargin)
            iter.foreach {
              case ((dt,area,city,adid),cnt) => {
                ps.setString(1,dt)
                ps.setString(2,area)
                ps.setString(3,city)
                ps.setString(4,adid)
                ps.setLong(5,cnt)
                ps.setLong(6,cnt)
                ps.executeUpdate()
              }
            }
            ps.close()
            conn.close()
          }
        )
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
}

