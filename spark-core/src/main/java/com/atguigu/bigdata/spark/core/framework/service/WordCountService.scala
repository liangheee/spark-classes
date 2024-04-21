package com.atguigu.bigdata.spark.core.framework.service

import com.atguigu.bigdata.spark.core.framework.common.TService
import com.atguigu.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author Hliang
 * @create 2023-07-12 18:46
 */
class WordCountService extends TService{
  private val wordCountDao = new WordCountDao()

  def execute() = {
    // TODO 执行业务操作
    val lines = wordCountDao.readFile("datas/word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    val groupWord: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

    val wordToCount: RDD[(String, Int)] = groupWord.map(
      tuple => {
        tuple._2.reduce(
          (t1, t2) => (t1._1, t1._2 + t2._2)
        )
      }
    )

    val tuples: Array[(String, Int)] = wordToCount.collect()
    tuples
  }

}
