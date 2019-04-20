package spark_study

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object s1 extends App {
  val appName = "spark_study"
  val master = "local[*]"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val sc = new SparkContext(conf)

  //Parallelize acts lazily.
  val data = Array(1, 2, 3, 4, 5)
  println(data)
  val distData = sc.parallelize(data, 4)
  println(distData)
  println(distData.fold(0)((a, b) => a + b))
}
