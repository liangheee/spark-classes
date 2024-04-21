package com.atguigu.bigdata.spark.core.framework.controller

import com.atguigu.bigdata.spark.core.framework.common.TController
import com.atguigu.bigdata.spark.core.framework.service.WordCountService

/**
 * TODO 调度层
 * @author Hliang
 * @create 2023-07-12 18:46
 */
class WordCountController extends TController{
  private val wordCountService = new WordCountService();

  def dispatch(): Unit = {
    val tuples = wordCountService.execute()
    tuples.foreach(println)
  }
}
