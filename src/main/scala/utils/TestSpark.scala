package utils

import org.apache.spark.rdd.RDD

object TestSpark {

  def main(args: Array[String]): Unit = {
    println("kkkkk")
  }

  def aucCal(pred: RDD[(Long, Double)], realVal: RDD[(Long, Double)]): Double = {
    //join 操作
    val pre = pred.zipWithIndex
    val label = realVal.zipWithIndex
    val combPair = pre.join(label)
    //计算正样本的ranker之和
    val sortPair = combPair.map(pair => pair._2).sortBy(pair => pair._1, ascending = true).zipWithIndex
    val posSum = sortPair.filter(pair => pair._1._2 == 1).map(pair => pair._2).sum()
    //计算正样本数量M和负样本数量N

    val M = sortPair.filter(pair => pair._1._2 == 1).count
    val N = sortPair.filter(pair => pair._1._2 == 0).count
    //计算公式
    val auc = (posSum - ((M - 1.0) * M) / 2) / (M * N)
    auc
  }
}
