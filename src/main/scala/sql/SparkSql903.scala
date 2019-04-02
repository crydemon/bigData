package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.{FileUtils}

object SparkSql903 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\datas.csv")
    .createOrReplaceTempView("order_info")

  //16.720800495990105
  spark
    .sql(s"""
            | select
            | sum(oi.shop_price) / count(1)
            | from order_info oi
          """.stripMargin)
    .show()
}


object SparkSql903_1 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\datas.csv")
    .createOrReplaceTempView("order_info")

  val avg = 16.720800495990105
  spark
    .sql(s"""
            | select
            |   oi.user_id,
            |   case
            |     when sum(oi.shop_price) / count(1) <= 1/2 * ${avg} then '0-1/2'
            |     when sum(oi.shop_price) / count(1) <= ${avg} then '1/2-1'
            |     when sum(oi.shop_price) / count(1) <= 2 * ${avg} then '1-2'
            |     else '2-'
            |    end as distribution
            | from order_info oi
            | group by user_id
          """.stripMargin)
    .createOrReplaceTempView("ds")

  spark
    .sql(s"""
         | select
         |  distribution,
         |  count(1)
         | from ds
         | group by ds.distribution
    """.stripMargin)
    .show()

}


object SparkSql903_2 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\datas.csv")
    .createOrReplaceTempView("order_info")

  val avg = 16.720800495990105
  spark
    .sql(s"""
            | select
            |   oi.user_id,
            |   case
            |     when sum(oi.shop_price) / count(1) <= 1/2 * ${avg} then '0-1/2'
            |     when sum(oi.shop_price) / count(1) <= ${avg} then '1/2-1'
            |     when sum(oi.shop_price) / count(1) <= 2 * ${avg} then '1-2'
            |     else '2-'
            |    end as distribution
            | from order_info oi
            | group by user_id
          """.stripMargin)
    .createOrReplaceTempView("ds")

  spark
    .sql(s"""
            | select
            |  distribution,
            |  count(1)
            | from ds
            | group by ds.distribution
    """.stripMargin)
    .show()

}



object SparkSql903_3 extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\datas.csv")
    .createOrReplaceTempView("order_info")

  spark
  spark
    .sql(s"""
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
    .sql(s"""
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
    .sql(s"""
            | select
            |   case
            |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 15 then '0-15'
            |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 30 then '15-30'
            |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 45 then '30-45'
            |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 60 then '45-60'
            |     else '60-'
            |  end as distribution,
            |  f.user_id
            | from first f
            |   inner join second s using(user_id)
    """.stripMargin)
    .createOrReplaceTempView("ds")

  spark
    .sql(s"""
            | select
            |  distribution,
            |  count(1)
            | from ds
            | group by ds.distribution
    """.stripMargin)
    .show()
}


object SparkSql903_4 extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\datas.csv")
    .createOrReplaceTempView("order_info")

  spark
    .sql(s"""
            | select
            |   oi.user_id,
            |   count(1) as purchase_freq
            | from order_info oi
            | group by user_id
          """.stripMargin)
    .createOrReplaceTempView("ds")

  spark
    .sql(s"""
            | select
            |   case
            |     when purchase_freq <= 1 then '1'
            |     when purchase_freq <= 2 then '2'
            |     when purchase_freq <= 3 then '3'
            |     when purchase_freq <= 4 then '4'
            |     when purchase_freq <= 5 then '5'
            |     when purchase_freq <= 6 then '6'
            |     when purchase_freq <= 7 then '7'
            |     else '7-'
            |   end as distribution,
            |   count(1)
            | from ds
            | group by distribution
          """.stripMargin)
    .show()

}




