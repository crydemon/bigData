package sql

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql3598 {

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
    val user_reg = spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\注册信息.csv")

    val devices = user_reg
      .select("userDeviceInfo")
      .collectAsList()
      .toArray().mkString(",")
      .replace("[", "'")
      .replace("]", "'")

    println(devices)

    spark
      .read
      .jdbc(url, "app_event_log_register_login", prop)
      .where("device_id in " + "(" + devices + ")")
      .join(user_reg, $"device_id" === $"userDeviceInfo")
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\result")
  }

}
