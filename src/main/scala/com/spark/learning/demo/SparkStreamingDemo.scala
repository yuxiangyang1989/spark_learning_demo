package com.spark.learning.demo

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dongyu on 17/7/20.
  */
object SparkStreamingDemo {

  def main(args: Array[String]): Unit = {

    // 初始化配置
    val sparkConf = new SparkConf()
      .setAppName("SparkStreamingDemo")
      .setMaster("local[2]")
    // 初始化sparkContext
    val sc = new SparkContext(sparkConf)
    // 初始化streamingContext
    val ssc = new StreamingContext(sc, Seconds(10))
    // 设置参数
    val topics = "test"
    val numThreads = 1
    val group = "testGroup"   // 自己另外设置group
//    val zkQuorum = "localhost:2181"
    val zkQuorum = "192.168.1.138:2181"
    val topicMap = topics.split(",").map((_, numThreads)).toMap
    //设置kafka的topic,kafka 里面的分组group，zkQuorum 指的是zookee集群
    // line: yuxia,18,female,2342342,chengdu
    val kafkaStream: ReceiverInputDStream[(String, String)]
      = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

//    kafkaStream.print()     // 需要替换成统计逻辑并且输出至文件

    val result = kafkaStream.map(_._2.split(",")(0)).map((_, 1))
      .reduceByKey((left, right) => left+right)

    val time = System.currentTimeMillis()
//    result.print()

    // 结果输出至kafka另外一个topic
    result.saveAsTextFiles(s"files/result/$time")

    ssc.start()
    ssc.awaitTermination()

  }
}