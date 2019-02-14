package sql

import java.io.File

import org.apache.spark.sql.{SparkSession, functions}
import utils.FileUtils

object AnalysisData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    FileUtils.dirDel(new File("D:/result"))
    import spark.implicits._



    FileUtils.dirDel(new File("d:\\result_ios_20"))
    FileUtils.dirDel(new File("d:\\result_ios_18"))

    val ios20 = spark
      .read
      .option("header", "true")
      .csv("d:\\ios20.csv")


    ios20.createOrReplaceTempView("ios_20")
    spark
        .sqlContext
        .sql("select url, first(referrer), first(device_type) ,first(app_version), count(*) as click_count " +
          "from ios_20 " +
          "group by url " +
          "order by click_count desc  " +
          "limit 10000 ")
      .write
      .option("header", "true")
      .csv("d:\\result_ios_20")


    val ios18 = spark
      .read
      .option("header", "true")
      .csv("d:\\ios18.csv")

    ios18.createOrReplaceTempView("ios_18")
    spark
      .sqlContext
      .sql("select url, first(referrer), first(device_type) ,first(app_version), count(*) as click_count " +
        "from ios_18 " +
        "group by url " +
        "order by click_count desc  " +
        "limit 10000 ")
      .write
      .option("header", "true")
      .csv("d:\\result_ios_18")

  }

}
