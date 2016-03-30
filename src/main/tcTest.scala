
/**
  * Created by jie on 3/24/16.
  */
import org.apache.spark.mllib.recommendation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}

object tcTest {

  def makeUserProductRDD(sc:SparkContext ,users:Iterable[Int] ,rawItemData:RDD[String],K:Int,categoryPredictions: RDD[Rating]): RDD[(Int, Int)] ={
    var userProduct = List[(Int,Int)]()
    for(user<-users){
      var preferredCategory = scala.collection.mutable.Map[Int,Double]()
      for(category<-findTopKCategoryForUser(user,categoryPredictions,K))
      //所以这里加的时user,rating,没有category的值
        preferredCategory += category
      val products = rawItemData.map(line=>(line.split(",")(0).toInt,line.split(",")(2).toInt)).distinct().filter{
        //这里有问题,itemData中只有user,item项,没有category项,所以下面这样写肯定没有
        //我想的时先求出每个用户的category,然后再在UserData中挑出这些category的user,item训练,生成model,在prediction这样减少数据量了
        case (product,category)=>
          preferredCategory.contains(category)
      }.map{case (product,category)=>product}.collect()
      for(product<-products)
        userProduct = userProduct.:: (user,product)
    }
    sc.makeRDD(userProduct)
  }


  def findTopKCategoryForUser(user:Int,categoryPredictions: RDD[Rating],K:Int): Array[(Int, Double)] ={
    //这里返回的时user, rating,不是user,category
    val  resultData = categoryPredictions.filter(rating=> rating.user.equals(user)).map(r=>(r.user,r.rating)).collect()
      .sortWith(_._2>_._2)
    if(resultData.length < K)
      resultData
    else
      resultData.take(K)
  }

  def main(args:Array[String]): Unit = {
    if (args.length < 2) {
      println("Args missing")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("tianchTest1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawUserData = sc.textFile(args(0),9)
    val rawItemData = sc.textFile(args(1),9)
    val userProduct: RDD[(Int, Int)] = rawUserData.map(line=>(line.split(",")(0).toInt,line.split(",")(1).toInt)).distinct().persist()
    //println(userProduct.count()+"条需要预测的数据")
    val userCategory: RDD[(Int, Int)] = rawUserData.map(line=>(line.split(",")(0).toInt,line.split(",")(4).toInt)).distinct().persist()
    val userCategoryModel: MatrixFactorizationModel = MatrixFactorizationModel.load(sc, "/user/tianchiCFUserCategoryModel")
    val userProductModel = MatrixFactorizationModel.load(sc, "/user/tianchiCFUserProductModel")
    val productPredictions: RDD[recommendation.Rating] = userProductModel.predict(userProduct).persist()
    val categoryPredictions: RDD[Rating] = userCategoryModel.predict(userCategory).persist()
    val K = 100;//Below top K category will be dropped
    println("Predictions has been calculated")
    val users: Iterable[Int] = userProduct.map{
      case (user,product)=>
        (user,1)
    }.distinct().collectAsMap().keys

    //var lst = Array[(Int, Int)]()
    val step = users.size / 50
    for(i<-0 to 49){
      var lst = Array[(Int, Int)]()
      val partUsers = users.slice(i*step,(i+1)*step)
      val partUserProductRDD = makeUserProductRDD(sc,partUsers,rawItemData,K,categoryPredictions)
      println("making rdd completed")
      val partResultPredictions = userProductModel.predict(partUserProductRDD)
      for(user<-partUsers){
        val userPredictionsMeans = productPredictions.filter(r=>r.user.equals(user)).map(a=>a.rating).mean()
        val rst = partResultPredictions.filter{case r=>
          r.user.equals(user)&&r.rating > userPredictionsMeans*1.2
        }.map(r=>(r.user,r.product)).collect()
        lst = lst.++:(rst)
      }
      lst.take(10).foreach(println)
      sc.makeRDD(lst).repartition(1).saveAsTextFile("/user/tianchiOutput/result" + i + ".txt")
    }
    println("Calculating is over")
    //sc.makeRDD(lst).repartition(1).saveAsTextFile("/user/tianchiOutput/result.txt")
    //lst.take(10).foreach(println)

    sc.stop()
  }

}
