package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql3660 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    FileUtils.dirDel(new File("D:/result"))
    import spark.implicits._

    val users_time = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\druidStayTime.csv")

    users_time.createOrReplaceTempView("users_time")



    spark.sql("select " +
      "user_unique_id, " +
      "min(date_format(__time, 'y-MM-dd hh:mm:ss')) as entrance_time, " +
      "max(date_format(__time, 'y-MM-dd hh:mm:ss')) as leave_time, " +
      "unix_timestamp(max(date_format(__time, 'y-MM-dd hh:mm:ss'))) - unix_timestamp(min(date_format(__time, 'y-MM-dd hh:mm:ss'))) as stay_time_second " +
      "from users_time " +
      "group by user_unique_id "
    )
      .coalesce(1)
      .write
      .option("delimiter", ",")
      .option("header", "true")
      .csv("d://result")

  }

}
