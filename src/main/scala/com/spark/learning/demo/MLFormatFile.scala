package com.spark.learning.demo

import java.io.{PrintWriter, File}

import scala.io.Source

/**
  * Created by dongyu on 17/7/21.
  */
object MLFormatFile {

  def main(args: Array[String]) {

    val inputFile = "files/HR_Comma_sep.csv" //输入文件
    val outputFile = "files/HR_Comma_sep_libSVM.txt"  //格式化后的输出

    val formatLibSVM = Source.fromFile(inputFile).getLines() map {line =>
      val Array(satisfaction_level,
        last_evaluation,
        number_project,
        average_montly_hours,
        time_spend_company,
        work_accident,
        left,
        promotion_last_5years,
        sales,
        salary) = line.split(",", -1).map(_.trim)

      val label = left

      val format_salary = salary match {
        case "low" => "1"
        case "medium" => "2"
        case "high" => "3"
        case _ => "0"
      }
      val format_sales = sales match {
        case "sales" => "10"
        case "accounting" => "1"
        case "hr" => "2"
        case "technical" => "3"
        case "support" => "4"
        case "management" => "5"
        case "IT" => "6"
        case "product_mng" => "7"
        case "marketing" => "8"
        case "RandD" => "9"
        case _ => "0"
      }
      val infoArray = Array[String](satisfaction_level,
        last_evaluation,
        number_project,
        average_montly_hours,
        time_spend_company,
        work_accident,
        promotion_last_5years,
        format_sales,
        format_salary).zipWithIndex map {case (s, i) =>
          s"${i+1}:$s"
      }
      val result = label + " " + infoArray.mkString(" ") + "\n"
      result
    }

    val writer = new PrintWriter(new File(outputFile))
    formatLibSVM foreach (writer.write(_))
    writer.close()
  }
}
