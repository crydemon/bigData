package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql884 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  FileUtils.dirDel(new File("D:/result"))

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\884.csv")
    .createOrReplaceTempView("hit")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\open.csv")
    .createOrReplaceTempView("open")


  spark.sql("" +
    "select date, " +
    "count(1) " +
    "from hit h " +
    "inner join open o on h.device_id = o.device_id and h.date = o.push_date " +
    "group by date ")
    .show()

}


object SparkSql884_1 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  FileUtils.dirDel(new File("D:/result"))

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\884.csv")
    .createOrReplaceTempView("hit")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\install.csv")
    .createOrReplaceTempView("install")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\open.csv")
    .createOrReplaceTempView("open")


  spark.sql("" +
    "select date, " +
    "count(1) " +
    "from hit h " +
    "inner join open o on h.device_id = o.device_id and h.date = o.push_date " +
    "inner join install i on h.device_id = i.device_id and datediff(h.date, i.install_date) = 1 " +
    "group by date ")
    .show()

}
