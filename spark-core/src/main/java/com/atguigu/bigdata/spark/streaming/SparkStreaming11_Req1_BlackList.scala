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
object SparkStreaming11_Req1_BlackList {
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

    // TODO 获取黑名单，判断该批次内的用户是否在黑名单中（周期性的操作）
    //        在黑名单中则过滤抛弃当前数据，不在黑名单中则进行下一步判断
    val filterRDD = kafkaDStream.transform(
        rdd => {
        // 获取黑名单
        val conn = JDBCUtil.getConnection
        val ps = conn.prepareStatement(
          """
            | select userid from black_list
            |""".stripMargin)
        val resultSet = ps.executeQuery()
        // 创建黑名单容器
        val blackList = ListBuffer[String]()
        while (resultSet.next()) {
          blackList += resultSet.getString(1)
        }
        resultSet.close()
        ps.close()
        conn.close()

        val filterRDD = rdd.filter(
          data => {
            val datas = data.split(" ")
            !blackList.contains(datas(3))
          }
        )
        filterRDD
      }
    )

    // TODO 将该批次内的用户对于相同广告的点击次数进行聚合
    val adCountRDD = filterRDD.map(
      data => {
        val datas = data.split(" ")
        val sdf = new SimpleDateFormat("yyyy-MM-dd: HH")
        val dateStr = sdf.format(new Date(datas(0).toLong))
        ((dateStr, datas(3), datas(4)), 1)
      }
    ).reduceByKey(_ + _)


    adCountRDD.foreachRDD(
      rdd => {
        // TODO Executor分布式运行，这里其实存在一定的并发问题
        rdd.foreach {
          case ((dt,userid,adid),cnt) => {
            // TODO 判断聚合后的点击次数是否超过阈值
            //        如果超过阈值，则将用户拉进黑名单，否则进行下一步判断
            if(cnt > 30){
              // 将用户拉进黑名单
              val conn = JDBCUtil.getConnection
              val ps = conn.prepareStatement(
                """
                  | insert into black_list (userid) values(?)
                  | on DUPLICATE KEY
                  | update userid = ?
                  |""".stripMargin)
              ps.setString(1,userid)
              ps.setString(2,userid)
              ps.executeUpdate()
              ps.close()
              conn.close()
            }

            // TODO 更新数据库中的用户针对某广告的点击次数
            //        这里需要进一步判断当前用户针对该广告更新后的点击次数是否超过阈值
            //            如果超过阈值，则将用户拉进黑名单
            val conn = JDBCUtil.getConnection
            val ps = conn.prepareStatement(
              """
                | select count from user_ad_count
                | where userid = ?
                | and adid = ?
                | and dt = ?
                |""".stripMargin)
            ps.setString(1,userid)
            ps.setString(2,adid)
            ps.setString(3,dt)
            val resultSet = ps.executeQuery()
            if(resultSet.next()){
              // 如果存在数据，则更新数据库
              val ps1 = conn.prepareStatement(
                """
                  | update user_ad_count
                  | set count = count + ?
                  | where dt = ? and userid = ? and adid = ?
                  |""".stripMargin)
              ps1.setString(1,String.valueOf(cnt))
              ps1.setString(2,dt)
              ps1.setString(3,userid)
              ps1.setString(4,adid)
              ps1.executeUpdate()
              ps1.close()

              // 判断更新后的数据是否超过阈值，如果超过则拉进黑名单
              val ps2 = conn.prepareStatement(
                """
                  | select count from user_ad_count
                  | where dt = ? and userid = ? and adid = ?
                  |""".stripMargin)
              ps2.setString(1,dt)
              ps2.setString(2,userid)
              ps2.setString(3,adid)
              val resultSet1 = ps2.executeQuery()
              while(resultSet1.next()){
                val count = resultSet1.getLong(1)
                // 更新后的值超过阈值，拉入黑名单
                if(count > 30){
                  val ps3 = conn.prepareStatement(
                    """
                      | insert into black_list (userid) values(?)
                      | on DUPLICATE KEY
                      | update userid = ?
                      |""".stripMargin)
                  ps3.setString(1,userid)
                  ps3.setString(2,userid)
                  ps3.executeUpdate()
                  ps3.close()
                }
              }
              resultSet1.close()
              ps2.close()
            }else{
              // 如果不存在数据，则插入新的记录
              val ps4 = conn.prepareStatement(
                """
                  | insert into user_ad_count (dt,userid,adid,count)
                  | values (?,?,?,?)
                  |""".stripMargin)

              ps4.setString(1,dt)
              ps4.setString(2,userid)
              ps4.setString(3,adid)
              ps4.setString(4,String.valueOf(cnt))
              ps4.executeUpdate()
              ps4.close()
            }

            resultSet.close()
            ps.close()
            conn.close()
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

