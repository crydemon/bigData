import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Row, SparkSession}

object mapTest extends App {
  def f(x: Int) = if (x > 2) Some(x) else None

  val l = List.range(1, 5)
  l.map(f(_)).foreach(println)
  println("=======flatMap==========")
  l.flatMap(f(_)).foreach(println)

  def g(x: Int) = if (x > 2) Some(List(x)) else None

  l.flatMap(g(_)).foreach(println)
}

object flatMapGroupByTest extends App {
  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate

  //spark.read.json("d:/train_stopover.txt").orderBy("duration_date", "station_sequence").show()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val ds = Seq(("99227100", "42222245", "2018-04-26"),
    ("99227100", "42222245", "2018-05-01"),
    ("34011381", "42830849", "2015-12-20"),
    ("34011381", "42830849", "2016-11-27"),
    ("34011381", "42830849", "2016-12-19"),
    ("34011381", "42830849", "2017-08-05")).toDS()

  //.toDF("ckey", "twkey", "s_date")

  ds.show()

  def encoder(columns: Seq[String]): Encoder[Row] = RowEncoder(StructType(columns.map(StructField(_, StringType, nullable = false))))

  val outputCols = Seq("ckey", "twkey", "s_date")
  ds.groupByKey(_._1).count().show()
  val result = ds.groupByKey(_._1)
    .flatMapGroups((_, rowsForEach) => {
      val list1 = scala.collection.mutable.ListBuffer[Row]()
      for (elem <- rowsForEach) {
        list1.append(Row(elem._1 + elem._2, elem._2, elem._3))
      }
      list1
    })(encoder(outputCols)).toDF

  result.show()

}