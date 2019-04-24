package spark_study

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")
    // Optional, but may help avoid errors due to long lineage
    spark.sparkContext.setCheckpointDir("d:/tmp/")
    //spark.conf.set("spark.sql.crossJoin.enabled", "true")


    val base = "D:\\profiledata_06-May-2005.tar\\profiledata_06-May-2005\\"
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base + "artist_alias.txt")

//    println(rawUserArtistData.rdd
//      .map(_.split(' ')(0).toDouble)
//      .stats()
//      .toString())
//
//    println(rawUserArtistData.rdd
//      .map(_.split(' ')(1).toDouble)
//      .stats()
//      .toString())

    //最大的用户ID 和艺术家ID 分别为2443548 和10794401，都
    //远小于2147483647，因此没必要对这些ID 做进一步处理。

    //flatMap works applying a function that returns a sequence for each element in the list， and flattening the results into the original list
    //flatMap[T:(T=>GenTraversableOnce[U])]:List[U]
    //flatMap， Option None被忽略
    //map() 函数要求对每个输入必须严格返回一个值，因此这里不能用这个函数。另一
    //种可行的方法是用filter() 方法删除那些无法解析的行，但这会重复解析逻辑。当需要将
    //每个元素映射为零个、一个或更多结果时，我们应该使用flatMap() 函数，因为它将每个
    //输入对应的零个或多个结果组成的集合简单展开，然后放入到一个更大的RDD 中。它可
    //以和Scala 集合一起使用，也可以和Scala 的Option 类一起使用
//    val artistByID = rawArtistData.rdd.flatMap(line => {
//      val (id, name) = line.span(_ != '\t')
//      if (name.isEmpty) None
//      else
//        try Some((id.toInt, name.trim))
//        catch {
//          case _: NumberFormatException => None
//        }
//    })
    //artistByID.foreach(println)

//    println(artistByID.lookup(6803336).head)

//    val artistAlias = rawArtistAlias.rdd.flatMap { line =>
//      val tokens = line.split('\t')
//      if (tokens(0).isEmpty) {
//        None
//      } else {
//        Some((tokens(0).toInt, tokens(1).toInt))
//      }
//    }.collectAsMap()

//    Thread.sleep(10000)


    val runRecommender = new RunRecommender(spark)
    //runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
    //runRecommender.model(rawUserArtistData, rawArtistData, rawArtistAlias)
    runRecommender.evaluate(rawUserArtistData, rawArtistAlias)
    //runRecommender.recommend(rawUserArtistData, rawArtistData, rawArtistAlias)
  }

}

class RunRecommender(private val spark: SparkSession) {

  import spark.implicits._

  def preparation(
                   rawUserArtistData: Dataset[String],
                   rawArtistData: Dataset[String],
                   rawArtistAlias: Dataset[String]): Unit = {

    //rawUserArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map { line =>
      val Array(user, artist, _*) = line.split(' ')
      (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    userArtistDF.agg(min("user"), max("user"), min("artist"), max("artist")).show()

    val artistByID = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badID, goodID) = artistAlias.head
    artistByID.filter($"id" isin(badID, goodID)).show()
  }

  def model(
             rawUserArtistData: Dataset[String],
             rawArtistData: Dataset[String],
             rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

    val trainData = buildCounts(rawUserArtistData, bArtistAlias).cache()

    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10). //user-m *user- rank, product-rank , product-n, m个用户，n个产品， rank相关系数
      setRegParam(0.01).//标准的过拟合参数；值越大越不容易产生过拟合，但值太大会降低分解的准确度。
      setAlpha(1.0).//控制矩阵分解时，被观察到的“用户- 产品”交互相对没被观察到的交互的权重
      setMaxIter(5). //迭代次数，
      setUserCol("user").
      setItemCol("artist").
      setRatingCol("count").
      setPredictionCol("prediction").
      fit(trainData)

    trainData.unpersist()

    //model.userFactors.select("features").show(truncate = false)

    val userID = 2093760

    val existingArtistIDs = trainData.
      filter($"user" === userID).
      select("artist").as[Int].collect()

    val artistByID = buildArtistByID(rawArtistData)

    artistByID.filter($"id" isin (existingArtistIDs: _*)).show()

    val topRecommendations = makeRecommendations(model, userID, 5)
    topRecommendations.show()

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()

    artistByID.filter($"id" isin (recommendedArtistIDs: _*)).show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

  def evaluate(
                rawUserArtistData: Dataset[String],
                rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))

    val allData = buildCounts(rawUserArtistData, bArtistAlias)
    val Array(trainData, cvData) = allData.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val allArtistIDs = allData.select("artist").as[Int].distinct().collect()
    val bAllArtistIDs = spark.sparkContext.broadcast(allArtistIDs)

    val mostListenedAUC = areaUnderCurve(cvData, bAllArtistIDs, predictMostListened(trainData))
    println(mostListenedAUC)

    val evaluations =
      for (rank <- Seq(5, 30);
           regParam <- Seq(1.0, 0.0001);
           alpha <- Seq(1.0, 40.0))
        yield {
          val model = new ALS().
            setSeed(Random.nextLong()).
            setImplicitPrefs(true).
            setRank(rank).setRegParam(regParam).
            setAlpha(alpha).setMaxIter(20).
            setUserCol("user").setItemCol("artist").
            setRatingCol("count").setPredictionCol("prediction").
            fit(trainData)

          val auc = areaUnderCurve(cvData, bAllArtistIDs, model.transform)

          model.userFactors.unpersist()
          model.itemFactors.unpersist()

          (auc, (rank, regParam, alpha))
        }

    evaluations.sorted.reverse.foreach(println)

    trainData.unpersist()
    cvData.unpersist()
  }

  def recommend(
                 rawUserArtistData: Dataset[String],
                 rawArtistData: Dataset[String],
                 rawArtistAlias: Dataset[String]): Unit = {

    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
    val allData = buildCounts(rawUserArtistData, bArtistAlias).cache()
    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10).setRegParam(1.0).setAlpha(40.0).setMaxIter(20).
      setUserCol("user").setItemCol("artist").
      setRatingCol("count").setPredictionCol("prediction").
      fit(allData)
    allData.unpersist()

    val userID = 2093760
    val topRecommendations = makeRecommendations(model, userID, 5)

    val recommendedArtistIDs = topRecommendations.select("artist").as[Int].collect()
    val artistByID = buildArtistByID(rawArtistData)
    artistByID.join(spark.createDataset(recommendedArtistIDs).toDF("id"), "id").
      select("name").show()

    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

  def buildArtistByID(rawArtistData: Dataset[String]): DataFrame = {
    rawArtistData.flatMap { line =>
      val (id, name) = line.span(_ != '\t')
      if (name.isEmpty) {
        None
      } else {
        try {
          Some((id.toInt, name.trim))
        } catch {
          case _: NumberFormatException => None
        }
      }
    }.toDF("id", "name")
  }

  def buildArtistAlias(rawArtistAlias: Dataset[String]): Map[Int, Int] = {
    rawArtistAlias.flatMap { line =>
      val Array(artist, alias) = line.split('\t')
      if (artist.isEmpty) {
        None
      } else {
        Some((artist.toInt, alias.toInt))
      }
    }.collect().toMap
  }

  def buildCounts(
                   rawUserArtistData: Dataset[String],
                   bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
    rawUserArtistData.map { line =>
      val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
      val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
      (userID, finalArtistID, count)
    }.toDF("user", "artist", "count")
  }

  def makeRecommendations(model: ALSModel, userID: Int, howMany: Int): DataFrame = {
    val toRecommend = model.itemFactors.
      select($"id".as("artist")).
      withColumn("user", lit(userID))
    model.transform(toRecommend).
      select("artist", "prediction").
      orderBy($"prediction".desc).
      limit(howMany)
  }

  //一般AUC 超过0.9 是高分。
  //实际中一个常用的做法是把数据
  //集分成k 个大小差不多的子集，用k-1 个子集做训练，在剩下的一个子集上做评估。我们
  //把这个过程重复k 次，每次用一个不同的子集做评估。这种做法称为k 折交叉验
  //但MLlib 的辅助方法MLUtils.kFold() 在一
  //定程度上提供了对这项技术的支持。

  //该实现接受一个检验集CV 和一个预测函数，CV 集代表每个用户
  //对应的“正面的”或“好的”艺术家。预测函数把每个“用户- 艺术家”对转换为一个预
  //测Rating。Rating 包含了用户、艺术家和一个数值，这个值越高，代表推荐的排名越高。
  def areaUnderCurve(
                      positiveData: DataFrame,
                      bAllArtistIDs: Broadcast[Array[Int]],
                      predictFunction: (DataFrame => DataFrame)): Double = {

    // What this actually computes is AUC, per user. The result is actually something
    // that might be called "mean AUC".

    // Take held-out data as the "positive".
    // Make predictions for each of them, including a numeric score
    val positivePredictions = predictFunction(positiveData.select("user", "artist")).
      withColumnRenamed("prediction", "positivePrediction")

    // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
    // small AUC problems, and it would be inefficient, when a direct computation is available.

    // Create a set of "negative" products for each user. These are randomly chosen
    // from among all of the other artists, excluding those that are "positive" for the user.
    //ctrl + q 查看变量类型
    //auc来说其实就是随机抽出一对样本（一个正样本，一个负样本），然后用训练得到的分类器来对这两个样本进行预测，预测得到正样本的概率大于负样本概率的概率。
    val negativeData = positiveData.select("user", "artist").as[(Int, Int)].
      groupByKey { case (user, _) => user }.
      flatMapGroups { case (userID, userIDAndPosArtistIDs) =>
        val random = new Random()
        val posItemIDSet = userIDAndPosArtistIDs.map { case (_, artist) => artist }.toSet
        println(posItemIDSet)
        val negative = new ArrayBuffer[Int]()
        val allArtistIDs = bAllArtistIDs.value
        var i = 0
        // Make at most one pass over all artists to avoid an infinite loop.
        // Also stop when number of negative equals positive set size
        while (i < allArtistIDs.length && negative.size < posItemIDSet.size) {
          val artistID = allArtistIDs(random.nextInt(allArtistIDs.length))
          // Only add new distinct IDs
          if (!posItemIDSet.contains(artistID)) {
            negative += artistID
          }
          i += 1
        }
        println(negative)
        println("--------------------------")
        // Return the set with user ID added back
        negative.map(artistID => (userID, artistID))
      }.toDF("user", "artist")

    // Make predictions on the rest:
    val negativePredictions = predictFunction(negativeData).
      withColumnRenamed("prediction", "negativePrediction")

    // Join positive predictions to negative predictions by user, only.
    // This will result in a row for every possible pairing of positive and negative
    // predictions within each user.
    //总共有MxN个正负样本对（N为负样本个数）
    val joinedPredictions = positivePredictions.join(negativePredictions, "user").
      select("user", "positivePrediction", "negativePrediction").cache()

    // Count the number of pairs per user
    val allCounts = joinedPredictions.
      groupBy("user").agg(count(lit("1")).as("total")).
      select("user", "total")
    // Count the number of correctly ordered pairs per user
    val correctCounts = joinedPredictions.
      filter($"positivePrediction" > $"negativePrediction").
      groupBy("user").agg(count("user").as("correct")).
      select("user", "correct")

    // Combine these, compute their ratio, and average over all users
    val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer").
      select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc")).
      agg(mean("auc")).
      as[Double].first()

    joinedPredictions.unpersist()

    meanAUC
  }

  //有必要把上述方法和一个更简单方法做一个基准比对。举个例子，考虑下面的推荐方法：
  //向每个用户推荐播放最多的艺术家。
  def predictMostListened(train: DataFrame)(allData: DataFrame): DataFrame = {
    val listenCounts = train.groupBy("artist").
      agg(sum("count").as("prediction")).
      select("artist", "prediction")
    allData.
      join(listenCounts, Seq("artist"), "left_outer").
      select("user", "artist", "prediction")
  }

}