package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yaml.snakeyaml.Yaml
import utils.DataSource.loadTable

object SQLService {
  private val sqlPath = "/MySQLQuery/sparksql"


  def getDataFromMySQL(database: String, key: String): DataFrame = {
    lazy val spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    val tables = "(" + SQLService.getSQL(key) + ") AS tmp"
    loadTable(spark, database, tables)
  }


  def getSQL(key: String): String = {
    val prop = getProp(sqlPath)
    prop.get(key)
  }

  private def getProp(path: => String): java.util.Map[String, String] = {
    lazy val stream = getClass.getResourceAsStream(sqlPath)
    val yaml = new Yaml()
    val obj = yaml.load(stream).asInstanceOf[java.util.Map[String, String]]
    obj
  }

  def main(args: Array[String]): Unit = {
    println(getSQL("3904_install"))
  }
}


