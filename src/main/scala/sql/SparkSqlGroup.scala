package sql

import org.apache.spark.sql.{SparkSession, functions}

object SparkSqlGroup {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate();
    import spark.implicits._
    val open = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\open.csv")

    open
      .groupBy("uid", "platform", "media_source")
      .agg(
        functions.count("uid").alias("open_count")
      )
      .withColumn("open_nums", functions
        .when($"open_count" > 10, 11)
        .when($"open_count" > 5, 6)
        .otherwise($"open_count"))
      .coalesce(1)
      .groupBy("open_nums", "platform", "media_source")
      .agg(
        functions.count("open_nums").alias("peoples")
      )
      .select("platform", "media_source", "open_nums", "peoples")
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\result")
  }
}
