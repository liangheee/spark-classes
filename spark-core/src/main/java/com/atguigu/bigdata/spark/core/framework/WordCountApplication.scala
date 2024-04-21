package com.atguigu.bigdata.spark.core.framework

import com.atguigu.bigdata.spark.core.framework.common.TApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordCountController

/**
 * @author Hliang
 * @create 2023-07-12 18:47
 */
object WordCountApplication extends App with TApplication {
  start(){
    val wordCountController = new WordCountController()
    wordCountController.dispatch()
  }
}
