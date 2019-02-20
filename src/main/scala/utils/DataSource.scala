package utils

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSource {
  private val themisPath = "src/main/resources/themis"
  private val reportPath = "src/main/resources/mysql"

  private lazy val themisProp = getProp(themisPath)
  private lazy val reportProp = getProp(reportPath)

  def loadTable(spark: SparkSession, database: String, table: String): DataFrame = {
    val prop =
      if (database == "themis")
        themisProp
      else if (database == "report")
        reportProp
      else
        themisProp

    val url = prop.getProperty("url")
    spark
      .read
      .jdbc(url, table, prop)
  }

  private def getProp(path: => String): Properties = {
    val prop = new Properties()
    val in = new FileInputStream(new File(path))
    prop.load(in)
    in.close()
    prop
  }

  def main(args: Array[String]): Unit = {
    themisProp
    themisProp
  }
}
