package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 14:14
 */
object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    // mapPartitionsWithIndex：可以以分区为单位进行数据转换
    //                但是会将整个分区内的数据加载到内存中进行引用
    //                如果同一分区内的数据没有处理结束，存在对象的引用
    //                在内存较小，数据量较大的场合下，容易出现内存溢出
    // 新的特征就是会携带数据的分区索引
    val mpwRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index,iter) => {
        if(index == 1){
          iter
        }else{
          Nil.iterator
        }
      }
    )

    mpwRDD.collect().foreach(println)



    sc.stop()

  }

}
