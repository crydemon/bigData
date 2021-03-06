package sql

import java.io.{File, FileInputStream, FileWriter}
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


  var output = "date,dau\n"
  while (calendar.getTime.compareTo(endDate) <= 0) {
    val beginDate = fmt.format(calendar.getTime)
    val count = data
      .filter(functions.to_date($"event_time") === beginDate and (functions.hour($"event_time") <= 11))
      .withColumn("event_date", functions.to_date($"event_time"))
      .select("device_id")
      .distinct()
      .count()
    output += beginDate + "," + count + "\n"
    println(output)
    calendar.add(Calendar.DATE, 1)
  }
  println(output)
  val outFile = new FileWriter("d:/dau_12.csv", false)
  outFile.write(output)
  outFile.close()

  calendar.setTime(fmt.parse("2019-01-01"))
  var output1 = "date,dau\n"
  while (calendar.getTime.compareTo(endDate) <= 0) {
    val beginDate = fmt.format(calendar.getTime)
    val count = data
      .filter(functions.to_date($"event_time") === beginDate and ($"country" === "FR"))
      .withColumn("event_date", functions.to_date($"event_time"))
      .select("device_id")
      .distinct()
      .count()
    output1 += beginDate + "," + count + "\n"
    calendar.add(Calendar.DATE, 1)
  }
  println(output1)
  val outFile1 = new FileWriter("d:/dau_fr.csv", false)
  outFile1.write(output1)
  outFile1.close()

  calendar.setTime(fmt.parse("2019-01-01"))
  var output2 = "date,dau\n"
  while (calendar.getTime.compareTo(endDate) <= 0) {
    val beginDate = fmt.format(calendar.getTime)
    val count = data
      .filter(functions.to_date($"event_time") === beginDate and ($"country" === "FR") and (functions.hour($"event_time") <= 11))
      .withColumn("event_date", functions.to_date($"event_time"))
      .select("device_id")
      .distinct()
      .count()
    output2 += beginDate + "," + count + "\n"

    calendar.add(Calendar.DATE, 1)
  }
  println(output2)
  val outFile2 = new FileWriter("d:/dau_fr_12.csv", false)
  outFile2.write(output2)
  outFile2.close()
}


object SparkSql_search extends App {

  FileUtils.dirDel(new File("d:/result1"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val calendar = Calendar.getInstance()
  val now = calendar.getTimeInMillis / 1000

  val uv = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\uv.csv")

  uv.persist()


  println(uv.schema.map(f=>f.name))
  println(calendar.getTimeInMillis / 1000 - now)

}

object SparkSql_device_tag extends App {

  FileUtils.dirDel(new File("d:/result1"))


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")


  val users = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\users.csv")

  val payments = spark
    .read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\payments.csv")
  users
    .withColumn("event_date", $"date".substr(0, 10))
    .groupBy("event_date")
    .count()
    .show(40, truncate = false)

  users
      .select("user_unique_id")
    .distinct()
    .join(payments, $"user_id" === $"user_unique_id", "left")
    .groupBy($"page_code", $"media_source")
    .count()
    .coalesce(1)
    .write
    .option("header", "true")
    .option("delimiter", ",")
    .csv("D:\\result1")


}

