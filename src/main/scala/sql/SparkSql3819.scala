package sql

import java.io.File

import org.apache.spark.sql.{SparkSession, functions}
import utils.FileUtils


object SparkSql3819D extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  FileUtils.dirDel(new File("D:/result"))
  import spark.implicits._
  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\B.csv")

  println(users
      .select("user_id")
    .distinct()
    .count())
}


object SparkSql3819C extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  import spark.implicits._

  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\A1.csv")

  users
    .select("user_id")
    .distinct()
    .coalesce(1)
    .write
    .option("delimiter", ",")
    .option("header", "false")
    .csv("d://result")

}

object SparkSql3819A extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  import spark.implicits._

  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\A1.csv")

  users
    .filter($"pay_count" > 0)
    .coalesce(1)
    .write
    .option("delimiter", ",")
    .option("header", "true")
    .csv("d://result")

}

object SparkSql3819B extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  FileUtils.dirDel(new File("D:/result"))

  import spark.implicits._

  val druid = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\druidRecord.csv")

  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\A1.csv")

  users
    .groupBy("user_id", "register_date", "userId")
    .agg(
      functions.count("user_id")
    )
    .select("user_id", "register_date", "userId")
    .createOrReplaceTempView("users")


  druid
    .filter($"page_code" === "cart")
    .createOrReplaceTempView("carts")

  druid
    .filter($"page_code" === "checkout")
    .createOrReplaceTempView("checkouts")

  druid
    .filter($"page_code" === "payment_success")
    .createOrReplaceTempView("payment")

  druid
    .groupBy("user_unique_id", "event_date")
    .agg(functions.count("page_code"))
    .select("user_unique_id", "event_date")
    .createOrReplaceTempView("druid")

  spark
    .sql(
      "select " +
        "d.user_unique_id as user_id, " +
        "u.userId, " +
        "datediff(d.event_date, u.register_date) + 1 as alive_day, " +
        "d.event_date as visiting_date, " +
        "if(c.page_code='cart', 'true', 'false') as cart, " +
        "if(ch.page_code='checkout', 'true', 'false') as checkout, " +
        "if(p.page_code='payment_success', 'true', 'false') as payment_success " +
        "from druid d " +
        "inner join users u on u.user_id = d.user_unique_id " +
        "left join carts c on c.user_unique_id = d.user_unique_id and c.event_date = d.event_date " +
        "left join checkouts ch on ch.user_unique_id = d.user_unique_id and ch.event_date = d.event_date " +
        "left join payment p on p.user_unique_id = d.user_unique_id and p.event_date = d.event_date "
    ).coalesce(1)
    .write
    .option("delimiter", ",")
    .option("header", "true")
    .csv("d://result")


}
