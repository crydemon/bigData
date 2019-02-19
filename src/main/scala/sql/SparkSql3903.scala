package sql

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql3903 extends App {
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

  println(url)
  println(prop)

  val table = "(SELECT " +
    "inviter_user_id " +
    "FROM free_sale_invite_record fsir " +
    "INNER JOIN app_event_log_start_up su ON su.uid = fsir.inviter_user_id " +
    "WHERE fsir.create_time >= '2019-02-05'      " +
    "AND fsir.create_time < '2019-02-12'      " +
    "AND su.event_time >= '2019-02-12' " +
    "AND su.event_time < '2019-02-19') as users "

  val table1 = "SELECT su.uid " +
    "FROM appsflyer_record ar " +
    " LEFT JOIN app_event_log_start_up su ON su.device_id = ar.device_id " +
    "WHERE ar.install_time >= '2019-02-16' " +
    "AND ar.install_time < '2019-02-19' " +
    "AND su.uid > 0"

  val table2 = "(SELECT inviter_user_id\nFROM free_sale_invite_record fsir\nWHERE fsir.create_time >= '2019-02-12'\n      AND fsir.create_time < '2019-02-18')" +
    " as users"

  spark.read.jdbc(url, table2, prop)
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")
}


object SparkSql3903_1 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\druidRecord.csv")
    .distinct()
    .createOrReplaceTempView("user_druid")

  spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\user_druid.csv")
    .distinct()
    .createOrReplaceTempView("user_all")

  spark.sql("select ua.user_unique_id " +
    "from user_all as ua " +
    "left join user_druid as ud on ua.user_unique_id = ud.inviter_user_id " +
    "where ud.inviter_user_id is null ")
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")

  import spark.implicits._

}


object SparkSql3903_2 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\users.csv")
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")

}
