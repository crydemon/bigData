package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("wordCount")

    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("192.168.4.239", 9999)
    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word=>(word, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
