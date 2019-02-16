package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import sql.SparkSql3866.spark
import utils.FileUtils

object SparkSql3866 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\tmp.csv")
    .createOrReplaceTempView("tmp")

  spark.sql("" +
    "select " +
    "media_source, " +
    "count(distinct device_id), " +
    "count(distinct uid)" +
    "from tmp " +
    "group by media_source"
  )
    .coalesce(1)
    .write
    .option("delimiter", ",")
    .option("header", "true")
    .csv("d://result")


}


object SparkSql3866_1 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  println(spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\device.csv")
    .distinct()
    .count())
}