package sql

import java.io.{File, FileWriter}
import utils.DataSource._

import org.apache.spark.sql.{SparkSession}
import utils.FileUtils

object SparkSql3903 extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  FileUtils.dirDel(new File("D:/result"))

  import scala.io.Source

  val src = Source.fromFile("d://user_id0304.csv", "utf-8")
  val userSource = src.getLines
    .filter(x => x.matches("[0-9]*"))
    .map(l => (l + ", "))

  val out = new FileWriter("d://result.csv", false)
  val head = "user_id,email,language_id,language_code,country_id,country_code\n"
  out.write(head)

  var i = 1
  var users = ""
  var table3 = ""
  for (user <- userSource) {
    if (i % 10000 == 0) {
      users += "2"
      table3 = "(SELECT\n  u.user_id,\n u.email,\n  u.language_id,\n  l.code,\n  u.country,\n  r.region_code\nFROM users AS u\n  LEFT JOIN languages l ON l.languages_id = u.language_id\n  LEFT JOIN region r ON r.region_id = u.country\nWHERE u.user_id IN (" +
        users + "\n" +
        ")) as tmp"
      println(table3)
      loadTable(spark, "themis", table3)
        .distinct()
        .foreach(row => {
          val line = row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3) + "," + row.get(4) + "," + row.get(5) + "\n";
          out.write(line)
        })
      users = ""
    }
    users += user
    i = i + 1
  }
  users += "2"
  table3 = "(SELECT\n  u.user_id,\n u.email,\n  u.language_id,\n  l.code,\n  u.country,\n  r.region_code\nFROM users AS u\n  LEFT JOIN languages l ON l.languages_id = u.language_id\n  LEFT JOIN region r ON r.region_id = u.country\nWHERE u.user_id IN (" +
    users + "\n" +
    ")) as tmp"

  loadTable(spark, "themis", table3)
    .distinct()
    .foreach(row => {
      val line = row.get(0) + "," + row.get(1) + "," + row.get(2) + "," + row.get(3) + "," + row.get(4) + row.get(5) + "\n";
      out.write(line)
    })

  out.close()
}

object test extends App {

  import scala.io.Source

  val src = Source.fromFile("d:/users.csv", "utf-8")

  src.getLines.map(l => (l + ", "))
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
