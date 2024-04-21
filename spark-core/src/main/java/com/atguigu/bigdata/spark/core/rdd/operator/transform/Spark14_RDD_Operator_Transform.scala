package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Hliang
 * @create 2023-06-27 15:08
 */
object Spark14_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)  partitionBy
    val rdd = sc.makeRDD(List(1,2,3,4))

    val mapRDD:RDD[(Int,Int)] = rdd.map((_, 1))

    // partitionBy不是RDD下面的方法，它是PairRDDFunctions下面的方法
    // 所以这里涉及了隐式转换  RDD 转换为 PairRDDFunctions
    /*
    implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
        new PairRDDFunctions(rdd)
    }
     */
    // partitionBy根据指定的分区规则对数据进行重分区
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
    // 如果对partitionBy后的RDD再进行partitionBy，是否还会进行再次重分区
    // 情况一：如果分区器相同，并且分区数也相同，则不会再次重分区
    // 情况二：如果分区器或者分区数二者有任一不同，则会触发重分区
    newRDD.partitionBy(new HashPartitioner(2))

    newRDD.saveAsTextFile("output")

    sc.stop()
  }

}
