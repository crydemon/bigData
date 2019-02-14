package utils

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSource {
  def getThemisTable(spark: SparkSession, table: String): DataFrame = {
    val prop = new Properties()
    val url = "jdbc:mysql://vovadb-slave-2.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis?useUnicode=true&characterEncoding=utf8"
    prop.setProperty("user", "qwang")
    prop.setProperty("password", "cjaHDKh@r4jdw")
    spark
      .read
      .jdbc(url, table, prop)
  }

  def getReportTable(spark: SparkSession, table: String): DataFrame = {
    val prop = new Properties()
    val url = "jdbc:mysql://vovadb-slave-2.cei8p8whxxwd.us-east-1.rds.amazonaws.com:3306/themis?useUnicode=true&characterEncoding=utf8"
    prop.setProperty("user", "qwang")
    prop.setProperty("password", "cjaHDKh@r4jdw")
    spark
      .read
      .jdbc(url, table, prop)
  }
}
