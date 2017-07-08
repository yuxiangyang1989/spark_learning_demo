package com.spark.learning.demo

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._

/**
  * Created by dongyu on 17/7/8.
  */
object Test {

//  case class Person(name:String, age:Int, gender:String, income:Long, addr:String)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set("spark.broadcast.compress", "false")


    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.textFile("files/person.txt")

//    rdd.foreach(println)


    // case class方式处理sql
//    val sqlRDD = rdd.map(line => {
//      line.split(",")
//    }).map( infos => Person(infos(0), infos(1).toInt,
//      infos(2), infos(3).toLong, infos(4)))
//      .toDF()  //新版本的spark必须转成DF之后才能用于sql
//
//    sqlRDD.registerTempTable("person")
//
//    sqlContext.sql("select * from person where age>20").foreach(println)


    // schema方式处理sql

    val schemaString = "name:String,age:Int,gender:String,income:Long,addr:String"

    val schemaType = schemaString.split(",").toList.map(_.split(":"))
      .map(infos => {
        infos(1) match {
          case "String" => StructField(infos(0), StringType, nullable = true)
          case "Int" => StructField(infos(0), IntegerType, nullable = true)
          case "Long" => StructField(infos(0), LongType, nullable = true)
        }
      })

    val schema = StructType(schemaType)

//    val sqlRDD = rdd.map(line => {
//      line
//    })


    val sqlRDD = rdd.map(_.split(","))
      .map(lines => Row(lines(0),lines(1).toInt,lines(2),lines(3).toLong,lines(4)))

    val peopleDF = sqlContext.createDataFrame(sqlRDD, schema)
    peopleDF.registerTempTable("person")
    sqlContext.cacheTable("person")
    sqlContext.sql("select * from person where age>20").foreach(println)










  }

}
