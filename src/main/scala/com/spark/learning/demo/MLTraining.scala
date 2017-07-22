package com.spark.learning.demo

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dongyu on 17/7/21.
  */
object MLTraining {

  def main(args: Array[String]) {

    val inputFile = "files/HR_Comma_sep_libSVM_train.txt"
    val modelFile = "files/HR_Comma_model"

    val sparkConf = new SparkConf()
      .setAppName("MLTrainning")
      .setMaster("local[2]")

    val sc = new SparkContext(sparkConf)

    val data = MLUtils.loadLibSVMFile(sc, inputFile)

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 21l)
    val training = splits(0)
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    model.clearThreshold()

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

//    model.save(sc, modelFile)

  }

}
