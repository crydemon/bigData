import org.apache.spark.sql.SparkSession

object GroupByKeyTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GroupByKey")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setCheckpointDir("E:\\checkpoint")

    val data = Array[(Int, Char)](
      (1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h')
    )

    val pairs = spark.sparkContext.parallelize(data, 3)

    pairs.checkpoint
    pairs.count

    val result = pairs.groupByKey(2)

    result.foreach(println)
    println(result.toDebugString)

  }
}
