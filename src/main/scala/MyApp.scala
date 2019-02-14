import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MyApp {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("wq")
      .master("local[*]")
      .getOrCreate()
    println("num lines: " + countLines(spark, args(0)))
  }

  def countLines(spark: SparkSession, path: String): Long = {
    spark.read.textFile(path).count()
  }
}
