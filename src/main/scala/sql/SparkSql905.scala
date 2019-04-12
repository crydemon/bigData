package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql905  extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\905.csv")
    .filter("length(device_id) > 5")
    .select("device_id")
    .show()

}


object SparkSql905_1  extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\905.csv")
    .filter("length(device_id) > 5")
    .select("device_id")
    .createOrReplaceTempView("druid")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\install_record.csv")
    .createOrReplaceTempView("install_record")

  spark
    .sql(s"""
            | select
            |   if(media_source IN('Facebook Ads', 'Organic', 'Unknown', 'googleadwords_int'), media_source, 'others') AS channel,
            |   count(1)
            | from druid d
            |   inner join install_record ir using(device_id)
            |  where ir.install_time >= '2019-03-27'
            | group by channel
          """.stripMargin)
    .show()

}


object SparkSql905_2  extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\905.csv")
    .filter("length(device_id) > 5")
    .select("device_id")
    .createOrReplaceTempView("druid")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\install_record.csv")
    .filter("install_time >= '2019-03-27'")
    .createOrReplaceTempView("install_record")

  spark
    .sql(s"""
            | select
            |   if(media_source IN('Facebook Ads', 'Organic', 'Unknown', 'googleadwords_int'), media_source, 'others') AS channel,
            |   count(1)
            | from druid d
            |   inner join install_record ir using(device_id)
            | group by channel
          """.stripMargin)
    .show()

}


//SELECT
//order_sn,
//order_id,
//order_time,
//payment_name,
//email,
//order_currency_id,
//oi.order_currency_id,
//r.region_code
//FROM order_info oi
//INNER JOIN region r ON r.region_id = oi.country
//INNER JOIN currency c ON c.currency_id = oi.order_currency_id
//WHERE oi.payment_id = 97
//AND oi.order_time >= '2019-04-05';
//
//DESC
//SELECT
//order_id,
//ifnull((SELECT platform FROM app_event_log_start_up su WHERE su.device_id = oi.device_id AND su.event_time >= '2019-04-01' LIMIT 1), 'Unknown') AS platform,
//ifnull((SELECT app_version FROM app_event_log_start_up su WHERE su.device_id = oi.device_id  AND su.event_time >= '2019-04-01' LIMIT 1), 'Unknown') AS app_version
//FROM order_info oi
//WHERE  oi.order_time >= '2019-04-05';

object SparkSql905_4  extends App{
  FileUtils.dirDel(new File("D:/result"))
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_info.csv")

    .createOrReplaceTempView("order_info")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\app_version.csv")
    .createOrReplaceTempView("app_version")

  spark
    .sql(s"""
            | select
            |   *
            | from order_info
            |   inner join app_version using(order_id)
          """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")

}