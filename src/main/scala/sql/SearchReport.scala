package sql

import java.io.File

import org.apache.spark.sql.SparkSession
import utils.FileUtils




object SparkSql_search_report extends App {

  FileUtils.dirDel(new File("d:/result"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._


  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\search_ctr.csv")
    .withColumn("pay_date", $"cur_day".substr(0, 10))
    .createOrReplaceTempView("uv_data")


  val order_goods = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_goods.csv")
    .filter("sku_pay_status >= 1 and parent_rec_id = 0")
    .withColumn("pay_date", $"pay_time".substr(0, 10))

  val order_cause = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_cause.csv")
    .filter("last_click_list_type is not null and trim(last_click_list_type) != '' ")
    .withColumnRenamed("user_id", "user_id_1")

  order_cause.createOrReplaceTempView("order_cause")

  order_goods
    .createOrReplaceTempView("order_goods")

  spark
    .sql(
      s"""
         | select
         |  pay_date,
         |  goods_sn,
         |  sum(price) AS goods_sn_gmv
         | from order_goods og
         | group by pay_date, goods_sn
    """.stripMargin)
    .createOrReplaceTempView("goods_sn_info")

  spark
    .sql(
      s"""
         | select *
         | from
         | (select
         |   *,
         |  row_number()
         |   over (partition by pay_date order by goods_sn_gmv desc)
         |     as rank
         | from goods_sn_info
         | ) as tmp
         | where tmp.rank <= 500
    """.stripMargin)
    .createOrReplaceTempView("tmp")


  spark
    .sql(
      s"""
         | select
         |  *
         | from order_goods og
         |  inner join order_cause oc using(rec_id)
    """.stripMargin)
    .createOrReplaceTempView("order_info")


  spark
    .sql(
      s"""
         | select
         |  pay_date,
         |  sum(order_goods_gmv) AS top_500_gmv
         | from order_info oi
         |  where oi.last_click_list_type = '/search_result'
         |    and exists (select * from tmp where tmp.goods_sn = oi.goods_sn and tmp.pay_date = oi.pay_date)
         | group by pay_date
    """.stripMargin)
    .createOrReplaceTempView("top_500")


  spark
    .sql(
      s"""
         | select
         |  oi.pay_date,
         |  round(sum(if(oi.last_click_list_type = '/search_result', oi.order_goods_gmv, 0)), 2) AS search_app_gmv,
         |  round(sum(oi.order_goods_gmv), 2) AS app_gmv,
         |  concat(round(sum(if(oi.last_click_list_type = '/search_result', oi.order_goods_gmv, 0)) * 100.0/ sum(oi.order_goods_gmv), 2), '%') AS search_rate
         | from order_info oi
         | group by pay_date
      """.stripMargin)
    .createOrReplaceTempView("all_gmv")


  spark
    .sql(
      s"""
         | select
         |  t.pay_date,
         |  a.*,
         |  u.uv,
         |  u.sum_clicks,
         |  u.sum_impressions,
         |  concat(round(u.rate * 100, 2), '%') AS search_ctr,
         |  round(search_app_gmv/ uv, 2) AS gmv_div_dau,
         |  round(top_500_gmv, 2) AS top_500_gmv,
         |  concat(round(top_500_gmv * 100.0 / app_gmv, 2), '%') AS top_500_rate
         | from top_500 t
         |  inner join all_gmv a using(pay_date)
         |  inner join uv_data u using(pay_date)
         | order by pay_date desc
    """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:/result")
}

