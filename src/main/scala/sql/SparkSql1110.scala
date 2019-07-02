package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql1110 extends App {
  FileUtils.dirDel(new File("d:/result1"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")


  val druid_users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\druid_users.csv")

  val genders = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\genders.csv")

  druid_users
    .withColumnRenamed("gender", "druid_gender")
    .withColumn("event_date", $"date".substr(0, 10))
    .join(genders, $"user_unique_id" === $"user_id")
    .groupBy("page_code", "gender")
    .count()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\result1")

}

object SparkSql1110_11 extends App {
  FileUtils.dirDel(new File("d:/result1"))
  FileUtils.dirDel(new File("d:/result2"))

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")


  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\users.csv")
    .withColumn("event_date", $"date".substr(0, 10))

  //users.groupBy("event_date", "os").count().distinct().show(truncate = false)

  val notification = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\notification.csv")

  val data = users
    .filter($"user_unique_id" >= 10000)
    .join(notification, $"user_unique_id" === $"user_id")

  data.cache()
  data.createOrReplaceTempView("data")

  spark.sql(
    """
      |select
      |  event_date,
      |  os,
      |  count(distinct user_id)
      |from data
      |where system_notification_status = 1
      |group by event_date, os
    """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\result1")

  spark.sql(
    """
      |select
      |  event_date,
      |  os,
      |  count(distinct user_id)
      |from data
      |where check_in_notification_status = 1
      |group by event_date, os
    """.stripMargin)

    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\result2")


  //
//  data.cache()
//
//  //`system_notification_status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '系统推送开关，1打开，0关闭',
//  //  `check_in_notification_status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '签到推送开关，1打开，0关闭',
//  data
//    .filter($"system_notification_status" === 1)
//    .select("event_date", "os", "user_id")
//    .groupBy("event_date", "os")
//    .count()
//    .distinct()
//    .coalesce(1)
//    .write
//    .option("header", "true")
//    .option("delimiter", ",")
//    .csv("D:\\result1")
//
//  data
//    .filter($"check_in_notification_status" === 1)
//    .select("event_date", "os", "user_id")
//    .groupBy("event_date", "os")
//    .count()
//    .distinct()

}


object SparkSql11101  {
  def main(args: Array[String]): Unit = {
    val appName = "druid"
    println(appName)
    val spark = SparkSession.builder
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //1196
    //d_1196(spark)
    val json = spark
      .read
      .textFile("d://vomkt-enrich-good-hose-1-2019-01-01-00-00-05-28c4a9b4-c7b3-4e7e-a060-ec92eddcfaa1.gz")

    spark.read.json(json).show(truncate = false)


    spark.close()
  }


}

