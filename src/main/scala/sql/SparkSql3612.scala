package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.{FileUtils}

object SparkSql3612 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()

    FileUtils.dirDel(new File("D:/result"))
    import spark.implicits._

    spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\reg_info.csv")
      .createOrReplaceTempView("reg_info")

    spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\app_reg_info.csv")
      .createOrReplaceTempView("app_reg_info")

    spark
      .read
      .option("header", "true")
      .option("delimiter", ",")
      .csv("d:\\risk_reg_info.csv")
      .createOrReplaceTempView("risk_reg_info")

    spark
      .sql("select ri.event_date, " +
        "ri.reg_total, " +
        "ri.reg_total_id, " +
        "round(ri.reg_total_id/ri.reg_total*100, 2)||'%' as reg_rate, " +
        "ri.sales_total, " +
        "round(ri.sales_total/ri.reg_total_id*100, 2)||'%' as order_rate, " +
        "ari.app_reg_total, " +
        "ari.app_reg_total_id, " +
        "round(ari.app_reg_total_id/ari.app_reg_total*100, 2)||'%' as app_reg_rate, " +
        "rri.reg_total as risk_reg_total, " +
        "rri.reg_total_id as risk_reg_total_id, " +
        "round(rri.reg_total_id/rri.reg_total*100, 2)||'%' as app_risk_reg_rate " +
        "from reg_info ri " +
        "left join app_reg_info ari using(event_date) " +
        "left join risk_reg_info rri using(event_date) ")
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\result")


  }
}
