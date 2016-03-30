import org.apache.spark.mllib.recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}



/**
  * Created by ASUS on 2016/3/21.
  */
object cfTest1 {

  def makeUserProductRDD(sc: SparkContext, users: RDD[Int], rawItemData: RDD[String], K: Int, categoryPredictions: RDD[Rating]): RDD[(Int, Int)] = {
    //var userProduct = List[(Int, Int)]()
    val ItemData = rawItemData.map(line => (line.split(",")(0).toInt, line.split(",")(2).toInt)).distinct()
        .map{
          case(item, cate) =>
            (cate, item)
        }.cache()
    val userPro = users.map{
      user =>
        val topK = findTopKCategoryForUser(user, categoryPredictions).map{
          case (user, cate) =>
            (cate, user)
        }.take(K)
        val userCategory = sc.makeRDD(topK)
        val uItem = ItemData.join(userCategory, 6).map{
          case(c, userItem) =>
            (userItem._2, userItem._1)
        }.collect()
        uItem
    }
    userPro.flatMap(x => x)
    /*
    for (user <- users) {
      var preferredCategory = scala.collection.mutable.Map[Int, Int]()
      for (category <- findTopKCategoryForUser(user, categoryPredictions, K))
        preferredCategory += category
      val products = rawItemData.map(line => (line.split(",")(0).toInt, line.split(",")(2).toInt)).distinct().filter {
        case (product, category) =>
          preferredCategory.contains(category)
      }.map { case (product, category) => product }.collect()
      for (product <- products)
        userProduct = userProduct.::(user, product)
    }
    // */
    //sc.makeRDD(userProduct)
  }


  def findTopKCategoryForUser(user: Int, categoryPredictions: RDD[Rating]): RDD[(Int, Int)] = {
    val resultData = categoryPredictions.filter(rating => rating.user.equals(user)).
      map(r => (r.user, r.product, r.rating)) //.collect().sortWith(_._3 > _._3).map(r => (r._2, r._1))
      .sortBy(_._3, false).map(r => (r._1, r._2))//.collect()
      resultData//.take(K)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Args missing")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("tianchTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawUserData = sc.textFile(args(0), 9)
    val rawItemData = sc.textFile(args(1), 9)
    val userProduct: RDD[(Int, Int)] = rawUserData.map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).distinct().persist()
    //println(userProduct.count()+"条需要预测的数据")
    val userCategory: RDD[(Int, Int)] = rawUserData.map(line => (line.split(",")(0).toInt, line.split(",")(4).toInt)).distinct().persist()
    val userCategoryModel: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, "/user/tianchiCFUserCategoryModel")
    val userProductModel = MatrixFactorizationModel.load(sc, "/user/tianchiCFUserProductModel")
    val productPredictions: RDD[recommendation.Rating] = userProductModel.predict(userProduct).persist()
    val categoryPredictions: RDD[Rating] = userCategoryModel.predict(userCategory).persist()
    val K = 100; //Below top K category will be dropped
    println("Predictions has been calculated")
    val users = userProduct.map {
      case (user, product) =>
        (user, 1)
    }.distinct().keys
    ////就是把所有用户的mean值先求出来
    val user = productPredictions.map {
      r => (r.user, r.rating)
    }.reduceByKey(_ + _).cache()
    val ucount = productPredictions.map {
      r => (r.user, 1)
    }.reduceByKey(_ + _).cache()

    val userMean = user.join(ucount).map {
      u => (u._1, u._2._1 / u._2._2)
    }//.keyBy(_._1).cache()
    val userProductRDD = makeUserProductRDD(sc, users, rawItemData, K, categoryPredictions)
    val resultPredictions = userProductModel.predict(userProductRDD)
      .map(r => (r.user, (r.product, r.rating))) //.keyBy(_._1).cache()
    val rst = resultPredictions.join(userMean).filter{
      case (user, predict) =>
        predict._1._2 > predict._2 * 1.2
    }.map{
      case (u, p) => (u, p._1._1)
    }
    rst.take(10).foreach(println)
    rst.saveAsTextFile("/user/tianchiOutput/UserItemResult.txt")

    /*
    //var lst = Array[(Int, Int)]()
    val step = users.size / 50
    for (i <- 0 to 19) {
      var lst = Array[(Int, Int)]()
      val partUsers = users.slice(i * step, (i + 1) * step)
      val partUserProductRDD = makeUserProductRDD(sc, partUsers, rawItemData, K, categoryPredictions)
      println("making rdd completed")
      val partResultPredictions = userProductModel.predict(partUserProductRDD)

      for (user <- partUsers) {
        val userPredictionsMeans = productPredictions.filter(r => r.user.equals(user)).map(a => a.rating).mean()
        val rst = partResultPredictions.filter { case r =>
          r.user.equals(user) && r.rating > userPredictionsMeans * 1.2
        }.map(r => (r.user, r.product)).collect()
        lst = lst.++:(rst)
      }


      lst.take(10).foreach(println)
      sc.makeRDD(lst).repartition(1).saveAsTextFile("/user/tianchiOutput/result" + i + ".txt")
    }


    println("Calculating is over")
    //sc.makeRDD(lst).repartition(1).saveAsTextFile("/user/tianchiOutput/result.txt")
    //lst.take(10).foreach(println)
    // */
    sc.stop()
  }


}
