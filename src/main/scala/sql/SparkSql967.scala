package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql967 extends App {
  FileUtils.dirDel(new File("D:/result"))
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_info.csv")
    .createOrReplaceTempView("order_info")

  val avg = 16.720800495990105
  spark
    .sql(
      s"""
         | select
         |   oi.user_id,
         |   case
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 1/2 * ${avg} then '0-1/2'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= ${avg} then '1/2-1'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 2 * ${avg} then '1-2'
         |     else '2-'
         |    end as avg
         | from order_info oi
         | group by user_id
          """.stripMargin)
    .createOrReplaceTempView("consume")


  spark
    .sql(
      s"""
         | select *
         | from
         | (select
         |   *,
         |  row_number()
         |   over (partition by user_id order by pay_time desc)
         |     as rank
         | from order_info
         | ) as tmp
         | where tmp.rank = 1
    """.stripMargin)
    .createOrReplaceTempView("first")

  spark
    .sql(
      s"""
         | select *
         | from
         | (select
         |   *,
         |  row_number()
         |   over (partition by user_id order by pay_time desc)
         |     as rank
         | from order_info
         | ) as tmp
         | where tmp.rank = 2
    """.stripMargin)
    .createOrReplaceTempView("second")

  spark
    .sql(
      s"""
         | select
         |   case
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 15 then '0-15'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 30 then '15-30'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 45 then '30-45'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 60 then '45-60'
         |     else '60-'
         |  end as intervals,
         |  f.user_id
         | from first f
         |   inner join second s using(user_id)
    """.stripMargin)
    .createOrReplaceTempView("activity")


  spark
    .sql(
      s"""
         | select
         |   oi.user_id,
         |   count(1) as freq
         | from order_info oi
         | group by user_id
          """.stripMargin)
    .createOrReplaceTempView("frequent")


  spark
    .sql(
      s"""
         | select
         |   *
         | from order_info oi
         |   inner join consume using (user_id)
         |   inner join activity using (user_id)
         |   inner join frequent using (user_id)
          """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")


}

object SparkSql967_1 extends App {
  FileUtils.dirDel(new File("D:/result"))
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\user_tag.csv")
    .select("device_id")
    .distinct()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:\\result")

}