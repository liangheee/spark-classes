package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author Hliang
 * @create 2023-07-12 18:58
 */
trait TDao {

  def readFile(path: String) = {
    val sc = EnvUtil.take()
    val lines: RDD[String] = sc.textFile(path)
    lines
  }
}
