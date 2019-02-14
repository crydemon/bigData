package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSqlPullGoodsInfo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    FileUtils.dirDel(new File("D:/female_rate"))
    import spark.implicits._

    val female_rate = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\female.csv")

    female_rate
      .filter(" impressions > 0")
      .withColumn("rate", $"clicks" / $"impressions")
      .orderBy($"rate".desc)
      .limit(100)
      .write
      .option("header", "true")
      .csv("D:\\female_rate")

    FileUtils.dirDel(new File("D:/male_rate"))
    val male_rate = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\male.csv")

    male_rate
      .filter(" impressions > 0")
      .withColumn("rate", $"clicks" / $"impressions")
      .orderBy($"rate".desc)
      .limit(100)

      .write
      .option("header", "true")
      .csv("D:\\male_rate")

  }

}
