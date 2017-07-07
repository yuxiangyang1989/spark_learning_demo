package com.spark.learning.demo

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dongyu on 17/7/7.
  */
object ReadFileDemo {

  def main(args: Array[String]) {

    // 初始化conf和sc
    val sparkConf = new SparkConf()
      .setAppName("ReadFileDemo")
      .setMaster("local[2]")
      .set("spark.broadcast.compress", "false")

  }

}
