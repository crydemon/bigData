package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql935 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\tmp.csv")
    .createOrReplaceTempView("app_message_push")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\appsflyer_record.csv")
    .createOrReplaceTempView("appsflyer_record")



  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\click_01.csv")
    .createOrReplaceTempView("click_01_t")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\click_24.csv")
    .createOrReplaceTempView("click_24_t")


  FileUtils.dirDel(new File("D:/result"))

  spark
    .sql(
      s"""
         | SELECT
         |   to_date(amp.push_date) AS push_date,
         |   ar.media_source,
         |   count(1)   AS num,
         |   sum(amp.switch_on) AS switch_on,
         |   sum(amp.success_push)  AS success_push
         | FROM app_message_push amp
         |   LEFT JOIN appsflyer_record ar using(device_id)
         | GROUP BY to_date(amp.push_date), media_source
          """.stripMargin)
      .createOrReplaceTempView("push_data")

  spark
    .sql(
      s"""
         | select
         |   push_date,
         |   media_source,
         |   num AS push_num,
         |   round(success_push * 1.0/ num, 4) AS push_success_rate,
         |   round(switch_on * 1.0/ success_push, 4)  AS os_switch_on_rate,
         |   click_01,
         |   round(click_01 * 1.0/ success_push, 4)  AS click_01_rate,
         |   click_24,
         |   round(click_24 * 1.0/ success_push, 4) AS click_24_rate
         | from push_data
         |   left join click_01_t using(push_date, media_source)
         |   left join click_24_t using(push_date, media_source)
          """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")
}
