package sql

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.{SparkSession, functions}
import utils.FileUtils

object SparkSql3570 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    FileUtils.dirDel(new File("D:/result"))
    import spark.implicits._

    val prop = new Properties()
    val file = new File("src/main/resources/mysql")
    val in = new FileInputStream(file)
    prop.load(in)
    val url = prop.getProperty("url")

    val users = spark
      .read
      .jdbc(url, "users", prop)
      .select("user_id", "gender", "reg_page")

    val registerLogin = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\freesale.csv")


    registerLogin
      .join(users, $"user_id" === $"uid")
      .select("user_id", "platform", "gender", "reg_page")
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\result")

  }

}
