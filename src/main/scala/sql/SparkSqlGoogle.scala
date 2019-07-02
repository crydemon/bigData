package sql

import org.apache.spark.sql.SparkSession

object SparkSqlGoogle  extends App {
  (21 :: (1 to 14).toList ::: 28.to(364, 28).toList).foreach(println)

}
