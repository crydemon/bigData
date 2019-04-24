package sql

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.functions
import org.apache.spark.sql.SparkSession
import utils.FileUtils

object SparkSql1006 extends App {
  FileUtils.dirDel(new File("D:/result"))
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()


  val prop = new Properties()
  val file = new File("src/main/resources/mysql")
  val in = new FileInputStream(file)
  prop.load(in)
  val url = prop.getProperty("url")

  val loginSql =
    s"""
       | (SELECT
       |   device_id,
       |   action_date
       | FROM temp_country_device_cohort_details tcdcd
       | WHERE action_date >= '2019-04-15'
       |       AND action_date < '2019-04-20'
       |  ) AS tmp
          """.stripMargin

  spark
    .read
    .jdbc(url, loginSql, prop)
    .createOrReplaceTempView("login")

  val orderInfoSql =
    s"""
       | (
       |   SELECT
       |    oi.device_id,
       |    oi.order_id,
       |    oi.order_time,
       |    oi.shipping_fee,
       |    oi.goods_amount,
       |    oi.bonus,
       |    oi.order_amount,
       |    oi.pay_time
       |  FROM (
       |         SELECT DISTINCT tcdcd.device_id
       |         FROM temp_country_device_cohort_details tcdcd
       |         WHERE tcdcd.action_date >= '2019-04-15') AS tmp
       |    INNER JOIN order_info oi ON tmp.device_id = oi.device_id
       |                                AND oi.pay_status IN (1, 2)
       |                                AND oi.email NOT LIKE '%@tetx.com'
       |  ) AS tmp
          """.stripMargin

  spark
    .read
    .jdbc(url, loginSql, prop)
    .createOrReplaceTempView("login")

  val avg = 16.720800495990105
  spark
    .sql(
      s"""
         | select
         |   oi.device_id,
         |   case
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 1/2 * ${avg} then '0-1/2_price'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= ${avg} then '1/2-1_price'
         |     when sum(oi.shipping_fee + oi.goods_amount) / count(1) <= 2 * ${avg} then '1-2_price'
         |     else '2-'
         |    end as avg
         | from order_info oi
         | group by device_id
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
         |   over (partition by device_id order by pay_time desc)
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
         |   over (partition by device_id order by pay_time desc)
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
         |     when s.pay_time is null then 'only ordered once'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 15 then '0-15'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 30 then '15-30'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 45 then '30-45'
         |     when datediff(to_date(f.pay_time), to_date(s.pay_time)) <= 60 then '45-60'
         |     else '60-'
         |  end as intervals,
         |  f.device_id
         | from first f
         |   left join second s using(device_id)
    """.stripMargin)
    .createOrReplaceTempView("activity")


  spark
    .sql(
      """
        | select
        |   oi.device_id,
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
        | group by device_id
      """.stripMargin)
    .createOrReplaceTempView("frequent")


  val device_tag = spark
    .sql(
      s"""
         | select
         |   *
         | from login
         |   left join consume using (device_id)
         |   left join activity using (device_id)
         |   left join frequent using (device_id)
          """.stripMargin)


  //视图
  List("avg", "intervals", "freq") foreach (key => {
    FileUtils.dirDel(new File(s"d:/$key"))
    device_tag
      .groupBy("action_date")
      .pivot(s"$key")
      .count()
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv(s"d:/$key")
  })


  device_tag.createOrReplaceTempView("tmp")

  val tmp = spark
    .sql(
      s"""
         | select
         |  tmp.*
         | from
         |  tmp
         |  inner join tmp tmp1 using(device_id)
         | where datediff(to_date(tmp1.action_date), to_date(tmp.action_date)) = 1
      """.stripMargin)

  //视图
  List("avg", "intervals", "freq") foreach (key => {
    FileUtils.dirDel(new File(s"d:/$key" + "_next_day"))
    tmp
      .groupBy("action_date")
      .pivot(s"$key")
      .count()
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv(s"d:/$key" + "_next_day")
  })


}

object SparkSql1006_4 extends App {


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\device_tags.csv")
    .createOrReplaceTempView("tmp")

  val tmp = spark
    .sql(
      s"""
         | select
         |  tmp.*
         | from
         |  tmp
         |  inner join tmp tmp1 using(device_id)
         | where datediff(to_date(tmp1.action_date), to_date(tmp.action_date)) = 1
      """.stripMargin)

  //视图
  List("avg", "intervals", "freq") foreach (key => {
    FileUtils.dirDel(new File(s"d:/$key"))
    tmp
      .groupBy("action_date")
      .pivot(s"$key")
      .count()
      .coalesce(1)
      .write
      .option("header", "true")
      .option("delimiter", ",")
      .csv(s"d:/{$key}_next_day")
  })


}


object SparkSql1006_5 extends App {

  FileUtils.dirDel(new File("d:/last_click_list_type"))

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_cause.csv")
    .createOrReplaceTempView("order_cause")

  spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\order_info.csv")
    .createOrReplaceTempView("order_info")

  val tmp = spark
    .sql(
      s"""
         | select
         |  to_date(oi.order_time) as order_date,
         |  last_click_list_type,
         |  pay_status,
         |  first(oi.user_id) as user_id
         |  count(1)
         | from order_info oi
         |  inner join order_cause oc on oc.order_goods_rec_id = oi.rec_id
         | where virtual_goods_id IN(7470721,7933041,6662967,8130590,7303124,7367251,7419699,7792377,7905515,8153360,5489099,7470311,5327351,5294164,8173631,5524538,4351394,7216360,8560696,7343476)
         |  group by order_date, last_click_list_type,pay_status
      """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:/last_click_list_type")


}


object SparkSql1022 extends App {

  FileUtils.dirDel(new File("d:/result"))

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  val orders = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\orders.csv")


  orders.createOrReplaceTempView("orders")

  val dau = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\dau.csv")
    .withColumnRenamed("count(DISTINCT device_id)", "dau")
    .withColumnRenamed("date(su.event_time)", "event_date")
    .withColumnRenamed("country", "country_code")

  dau.createOrReplaceTempView("daus")

  orders.show()
  dau.show()

  spark
    .sql(
      s"""
         | select
         |  d.event_date,
         |  d.country_code,
         |  concat(round(payed_num * 100.0/dau, 2), '%') AS CR
         | from daus d
         |  inner join orders o on d.event_date = o.event_date and d.country_code = o.country_code
      """.stripMargin)
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("d:/result")

}


object SparkSql_tmp1 extends App {

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
    .csv("D:\\uv.csv")
    .withColumn("pay_date", $"date".substr(0, 10))
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
    .withColumnRenamed("order_goods_rec_id", "rec_id")
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
         |  u.pv,
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


object SparkSql_tmp2 extends App {

  FileUtils.dirDel(new File("d:/result"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val pattern = "yyyy-MM-dd"
  val fmt = new SimpleDateFormat(pattern)
  val calendar = Calendar.getInstance()

  val data = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\login.csv")

  calendar.setTime(fmt.parse("2019-01-01"))
  val endDate = fmt.parse("2019-04-22")
  while (calendar.getTime.compareTo(endDate) <= 0) {
    val beginDate = fmt.format(calendar.getTime)
    val login = data
      .filter(functions.to_date($"event_time") === beginDate)
      .withColumn("event_date", functions.to_date($"event_time"))
      .groupBy("device_id").agg(functions.approx_count_distinct("device_id").alias("dau"))
    if (beginDate == "2019-01-01") {
      login.createOrReplaceTempView("result")
    } else {
      login.createOrReplaceTempView("tmp")
      spark.sql(
        """
          | select *
          |   from result
          |     union all select * from tmp
        """.stripMargin).createOrReplaceTempView("result")
    }

    if (beginDate == "2019-04-22") {
      spark.sql(
        """
          | select *
          |   from result
        """.stripMargin)
        .coalesce(1)
        .write
        .option("header", "true")
        .option("delimiter", ",")
        .csv("d:/result")
    }
    calendar.add(Calendar.DATE, 1)
  }

}


