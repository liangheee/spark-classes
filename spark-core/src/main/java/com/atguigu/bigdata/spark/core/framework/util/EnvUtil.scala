package com.atguigu.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author Hliang
 * @create 2023-07-12 18:54
 */
object EnvUtil {

  private val scLocal = new ThreadLocal[SparkContext]

  def put(sparkContext: SparkContext): Unit = {
    scLocal.set(sparkContext)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear() = {
    scLocal.remove()
  }

}
