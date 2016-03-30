import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by jie on 3/28/16.
  */
object tccategory {
  def calcu(para: (Double, String)): Double = {
    val br: Double = para._1
    val tm: String = para._2
    val rate = {
      if (tm <= "2014-11-27") {
        0.95 * 0.95
      } else if (tm <= "2014-12-07") {
        0.95
      } else {
        1.00
      }
    }
    Math.pow(br * rate, 2.0)
  }
  def makeUserProductRDD(sc:SparkContext ,users:Iterable[Int] ,products: Array[Int]): RDD[(Int, Int)] ={
    var userProduct = List[(Int,Int)]()
    for(user<-users;product<-products){
      userProduct = userProduct.:: (user,product)
    }
    sc.makeRDD(userProduct)
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Args missing")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("tianchiCategory")
    val sc = new SparkContext(conf)
    val rawUserData = sc.textFile(args(0), 9).cache()
    val rawItemData = sc.textFile(args(0), 9).cache()

    val ratings = rawUserData.map {
      line =>
        val data = line.split(",")
        val behavior = data(2).toDouble
        val date = data(5).split(",")(0)
        val user = data(0)
        val category = data(4)
        ((user, category), calcu(behavior, date))
    }.reduceByKey((x: Double, y: Double) => Math.sqrt(x + y)).map {
      case ((user, product), behavior) =>
        Rating(user.toInt, product.toInt, behavior)
    }
    val userProducts: RDD[(Int, Int)] = ratings.map {
      case Rating(user, product, behavior) =>
        (user, product)
    }.persist()
    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.01)
    println("The model has been constructed")
    //model.save(sc, "/user/tcUserCategoryModel")

    val userCategory: RDD[(Int, Int)] = rawUserData.map(line=>(line.split(",")(0).toInt,line.split(",")(4).toInt))
      .distinct().persist()
    val predictions = model.predict(userCategory).persist()

    val users = rawUserData.map(line => (line.split(",")(0).toInt, 1)).distinct().collectAsMap().keys
    val categoroy = rawItemData.map(line => (line.split(",")(2).toInt, 1)).reduceByKey(_ + _).map(x => x._1).collect()
    val UserCateogryPre = model.predict(makeUserProductRDD(sc, users, categoroy)).persist()

    var userPredictions = scala.collection.mutable.Map[Int,Double]()
    var userPrediction = scala.collection.mutable.Map[Int, Array[Int]]()
    //计算用户效果
    for(user <- users) {
      //userPredictionsMeans += (user->predictions.filter(r=>r.user.equals(user)).map(a=>a.rating).mean())
      val oneuserCategory =  predictions.filter(r=>r.user.equals(user)).
        map(a => (a.product, a.rating)).sortBy(x => x._2, false).map(a => (a._1)).collect()
      /*
      val oneuserCategory = UserCateogryPre.filter{
        case r =>
          r.user.equals(user) && r.rating > userPredictionsMeans.getOrElse(user,default = 2.5)*1.2
      }.map(r => r.product).sortBy(x => x, false).take(100)
      userPrediction += (user -> oneuserCategory)
      // */
      userPrediction += (user -> oneuserCategory)
    }
    println("User's predictions has been calculated")
    userPrediction.take(100).foreach(println)
    sc.makeRDD(userPrediction.toSeq).repartition(1).saveAsTextFile("/user/spark/tianchicf/userCategory.txt")
    sc.stop()
  }
}
