package sql

import java.io.File

import org.apache.spark.sql.{SparkSession, functions}
import utils.FileUtils
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions.{collect_list, lit, udf}


object SparkSql967 extends App {
  FileUtils.dirDel(new File("D:/result"))
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

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
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 1/2 * ${avg} then '0-1/2_price'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= ${avg} then '1/2-1_price'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 2 * ${avg} then '1-2_price'
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
      """
        | select
        |   oi.user_id,
        |   case
        |    when count(1) = 1 then '1_ordered'
        |    when count(1) = 2 then '2_ordered'
        |    when count(1) = 3 then '3_ordered'
        |    when count(1) = 4 then '4_ordered'
        |    when count(1) = 5 then '5_ordered'
        |   else
        |    'more_than_5_ordered'
        |   end as freq
        | from order_info oi
        | group by user_id
      """.stripMargin)
    .createOrReplaceTempView("frequent")


  spark
    .sql(
      s"""
         | select
         |   *
         | from order_info
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
    .createOrReplaceTempView("users")

  spark
    .sql(
      s"""
         | select
         |  device_id,
         |  first(avg) AS avg,
         |  first(intervals) AS intervals,
         |  first(freq) AS freq
         | from
         |  users
         | group by device_id
    """.stripMargin)
    .createOrReplaceTempView("user_tag")


  val visits = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\967.csv")

  val login = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\login.csv")
  import spark.implicits._


  val dateStr = "2019-03-31"
  val pattern = "yyyy-MM-dd"
  val fmt = new SimpleDateFormat(pattern)
  val calendar = Calendar.getInstance()
  calendar.setTime(fmt.parse(dateStr))
  (0 to 9).foreach(_ => {


    calendar.add(Calendar.DATE, 1)
    val d = fmt.format(calendar.getTime)
    visits
      .withColumn("good_date", $"date".substr(0, 10))
      .filter($"good_date" === d)
      .createOrReplaceTempView("visits")




    login
      .filter($"action_date" === d)
      .createOrReplaceTempView("login")


    spark
      .sql(
        s"""
           | select
           |  ug.*,
           |  tmp.device_id,
           |  page_code
           | from
           | (select
           |   *,
           |  row_number()
           |   over (partition by device_id order by date desc)
           |     as rank
           | from visits
           | ) as tmp
           |  inner join login using(device_id)
           |  inner join user_tag AS ug using(device_id)
           | where tmp.rank = 1
    """.stripMargin)
      .createOrReplaceTempView("tmp")

    val total = spark
      .sql(
        s"""
           | select
           |  count(*) AS nums
           | from login
    """.stripMargin)
      .first()
      .getLong(0)


    if (total > 0) {
      List("avg", "intervals", "freq") foreach (key => {
        spark
          .sql(
            s"""
               | select
               |  ${key},
               |  round(count(1)/${total}, 4) rate
               | from tmp
               | group by ${key}
    """.stripMargin)
          .createOrReplaceTempView("rates")
        spark
          .sql(
            s"""
               | select
               |  ${key},
               |  page_code,
               |  count(1) as nums
               | from tmp
               | group by ${key}, page_code
    """.stripMargin)
          .createOrReplaceTempView("user_info")

        spark
          .sql(
            s"""
               | select
               |  ${key},
               |  concat_ws('|',collect_set(page_code)) AS page_codes
               | from
               | (select
               |   *,
               |  row_number()
               |   over (partition by ${key}, page_code order by nums desc)
               |     as rank
               | from user_info
               | ) as tmp
               | where tmp.rank <= 10
               | group by ${key}
               |
    """.stripMargin)
          .createOrReplaceTempView("page_codes_t")

        spark
          .sql(
            s"""
               | select
               |  ${key},
               |  rate,
               |  page_codes
               | from rates
               |  inner join page_codes_t using(${key})
    """.stripMargin)
          .createOrReplaceTempView(s"tmp_${key}")
      })
      spark
        .sql(
          s"""
             | select
             |  *
             | from tmp_avg
             |  union all select * from tmp_intervals
             |  union all select * from tmp_freq
    """.stripMargin)
        .coalesce(1)
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .csv(s"d:\\$d")
    }
  })
}


object SparkSql967_2 extends App {
  FileUtils.dirDel(new File("D:/result"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._


  val visits = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\967.csv")
    .withColumn("good_date", $"date".substr(0, 10))
    .createOrReplaceTempView("druid")

  val login = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\login.csv")
    .createOrReplaceTempView("mysql")

  spark
    .sql(
      s"""
         | select
         |  *
         | from druid d
         |  inner join mysql m on m.device_id = d.device_id and d.good_date = m.action_date
    """.stripMargin)
    .groupBy("action_date").agg(functions.count(lit(1))).show()


}