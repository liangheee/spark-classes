//package com.atguigu.bigdata.spark.streaming
//
//import java.net.URI
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
//
///**
// * @author Hliang
// * @create 2023-07-22 16:54
// */
//object SparkStreaming08_Close {
//
//  def main(args: Array[String]): Unit = {
//    /*
//       线程的关闭：
//       val thread = new Thread()
//       thread.start()
//
//       thread.stop(); // 强制关闭
//
//     */
//
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
//    val ssc = new StreamingContext(sparkConf, Seconds(3))
//
//    val lines = ssc.socketTextStream("localhost", 9999)
//    val wordToOne = lines.map((_,1))
//
//    wordToOne.print()
//
//    ssc.start()
//
//    // 如果想关闭采集器，必须创建新的线程进行操作
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        // 优雅的关闭
//        // 计算节点不再接收新的数据，而是将现有采集到的数据处理结束后，优雅的关闭采集器和环境对象
//        // 通常我们用外部组件来控制SparkStreaming的数据采集
//        // Mysql：Table（stopSpark） =》Row =》data
//        // Redis：Data(K,V)
//        // Zk：/stopSpark
//
//        // 这里我们采用hadoop节点的方式进行模拟
//        val monitor = new SparkStreamingMonitor
//        while(true){
//          Thread.sleep(5000)
//          if(monitor.stopStreamingContext(ssc)){
//            System.exit(0)
//          }
//
//        }
//      }
//    }).start()
//
//
//    ssc.awaitTermination() // block  阻塞进程
//
//    // 这个位置关闭采集器是没用的
//  }
//
//  class SparkStreamingMonitor{
//    private val MONITOR_SPARK_NODE = "/stopSpark"
//
//    def stopStreamingContext(ssc: StreamingContext): Boolean ={
//      // 如果在初始化状态，则返回false
//      if( ssc.getState() == StreamingContextState.INITIALIZED ){
//        return false
//      }
//
//      val uri = new URI("hdfs://hadoop102:8020")
//      val conf = new Configuration()
//      val user: String = "liangheee"
//      // 获取hadoop客户端文件系统对象
//      val fileSystem = FileSystem.get(uri, conf, user)
//
//      // 判断是否存在/stopSpark节点
//      val isExisted: Boolean = fileSystem.exists(new Path(MONITOR_SPARK_NODE))
//
//      if( !isExisted ){
//        // 如果不存在，判断环境对象的状态是否为活动状态，则优雅的关闭ssc
//        if( ssc.getState() == StreamingContextState.ACTIVE ){
//          // 如果是活动状态，则关闭ssc
//          ssc.stop(true,true)
//          return true
//        }
//      }
//
//      fileSystem.close()
//
//      false
//
//    }
//  }
//}
//
