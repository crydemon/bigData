package sql

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.{FileUtils, SQLService}

object SparkSql39041 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  val path = "d://appsflyer_record"
  FileUtils.dirDel(new File(path))
  val data = SQLService.getDataFromMySQL("report", "3904_install")
  data.write
    .option("header", "true")
    .option("delimiter", ",")
    .csv(path)
}


object SparkSql39042 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  FileUtils.dirDel(new File("D:/result"))

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\app_install_record.csv")
    .createOrReplaceTempView("app_install_record")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\appsflyer_record.csv")
    .createOrReplaceTempView("appsflyer_record")

  spark.sql("" +
    " select to_date(ar.install_time) AS event_date, " +
    " ar.media_source, " +
    " count(1) AS active_num, " +
    " sum(if(air.push_result = 2, 1, 0)) AS push_num " +
    " from appsflyer_record ar " +
    " left join app_install_record air on ar.device_id = air.device_id  and datediff(air.push_time, ar.install_time) = 1 " +
    " group by event_date, media_source")
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")
}


