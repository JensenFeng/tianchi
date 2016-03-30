import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
  * Created by jie on 3/22/16.
  *
  * userid, itemid, behavior, geohash, category, time
  * item, geohash, category
  */
object userItem {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage filePath")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("tianchi Test")
    val sc = new SparkContext(conf)
    //filePath
    val rawData = sc.textFile(args(0))
    val userData = rawData.map(line =>
      (line.split(",")(0), line.split(",")(1), line.split(",")(2).toInt,
        line.split(",")(5).split(" ")(0)))
    val userPair = userData.map(x => ((x._1, x._2), (x._3, x._4))).
      partitionBy(new HashPartitioner(100)).persist()

    val userItem = userData.map(x => (x._1, x._2, calcu(x._3, x._4))).
      map(x => (Pair(x._1, x._2), x._3))
    val userWeight = userItem.reduceByKey(_  + _).map(line => (line._1, Math.sqrt(line._2)))
    userWeight.collect().foreach(println)

    sc.stop()
  }
  def calcu(para: (Int, String)): Double = {
    val br: Int = para._1
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
}
