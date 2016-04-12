import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by jie on 4/6/16.
  * userid, itemid, behavior, geohash, category, time
  * item, geohash, category
  */
object userPut {
    def main(args: Array[String]): Unit = {
      if(args.length < 2){
        println("Usage args")
        System.exit(1)
      }

      val conf = new SparkConf().setAppName("userPut")
      val sc = new SparkContext(conf)

      // user.csv
      val rawData = sc.textFile(args(0)).map{
        line =>
          (line.split(",")(0), line.split(",")(1), line.split(",")(2).toInt, line.split(",")(5))
      }.cache()
      // item.csv
      val itemData = sc.textFile(args(1)).map(line => (line.split(",")(0), 1)).cache()
      val timeData = rawData.map(line => line._4.split(" ")(0)).distinct().collect().sorted
      val timeLength = timeData.length - 1
      /*
      val lastDayData = rawData.filter(line => (line._3 == 4 && line._4 >= (args(2) + " 00"))).map{
        line => ((line._1, line._2), 1)
      }.distinct().cache()
      val lastDayItem = lastDayData.map{case(ui,c) => (ui._2, 1)}.cache()
      //lastDayData.repartition(1).saveAsTextFile("/user/userCart/LastDayData.txt")
      val lastDayLength = lastDayData.collect().length

      for(len <- 0 to timeLength) {
        val day = timeData.apply(len)
        val filterData = rawData.filter(line =>
          (line._3 == 3 || line._3 == 4) && line._4 > (day + " 00") && line._4 <= ("2014-12-18" + " 00"))
          .map {
          line =>
            ((line._1, line._2), line._3, line._4)
        }

        val userItem = filterData.groupBy(u => u._1).map {
          line =>
            val u = line._2.map(ui => (ui._2, ui._3))
            (line._1, u.toArray)
        }.filter {
          case (key, arr) =>

            def compare(x1: (Int, String), x2: (Int, String)): Boolean = {
              if (x1._2 < x2._2) true
              else if (x1._2 > x2._2) false
              else {
                if (x1._1 < x2._1) true
                else false
              }
            }
            val arry = arr.sortWith(compare)

            val len = arry.length - 1
            if (arry.last._1 == 3){
              /*
              for(i <- len to 0){
                val t = arry.apply(i)
                if(t._1 == 4){
                  if(t._2)
                }
              } // */
              ! arry.map(_._1).contains(4)
            }
            else false
        }.map{case(ui, b) => (ui._2, ui._1)}

        ///
        val uItem = itemData.join(userItem, 3).map(x => (x._2._2, x._1)).distinct()

        //val uItem = lastDayItem.join(userItem, 3).map(x => (x._2._2, x._1)).distinct()
        //

        //uItem.sortBy(_._1).repartition(1).saveAsTextFile("/user/userCart/" + day + ".txt")

        val dayItem = uItem.map(line => (line, 1))
        val dayLength = dayItem.collect().length

        val commonSet = dayItem.join(lastDayData, 3).distinct()
        //commonSet.repartition(1).saveAsTextFile("/user/userCart/CommonSet" + day + ".txt")
        val commonLen = commonSet.collect().length
        val recall = commonLen * 1.0 / lastDayLength
        val pre = commonLen * 1.0 / dayLength
        println(day + "commonSet: " + commonLen + " - recall: " + recall + " - pre: " + pre)
      }
      // */
      //*预测19号的
      var i = 0
      while(i < timeLength)
      {
        val day = timeData.apply(i)
        i = i + 7
        val filterData = rawData.filter(line =>
          (line._3 == 3 || line._3 == 4) && line._4 >= (day + " 00") && line._4 <= ("2014-12-19" + " 00"))
          .map {
            line =>
              ((line._1, line._2), line._3, line._4)
          }
        val userItem = filterData.groupBy(u => u._1).map {
          line =>
            val u = line._2.map(ui => (ui._2, ui._3))
            (line._1, u.toArray)
        }.filter {
          case (key, arr) =>
            def compare(x1: (Int, String), x2: (Int, String)): Boolean = {
              if (x1._2 < x2._2) true
              else if (x1._2 > x2._2) false
              else {
                if (x1._1 < x2._1) true
                else false
              }
            }
            val arry = arr.sortWith(compare)

            //val len = arry.length - 1
            if (arry.last._1 == 3) {
              !arry.map(_._1).contains(4)
            }
            else false
        }.map { case (ui, b) => (ui._2, ui._1) }
        val uItem = itemData.join(userItem, 3).map(x => (x._2._2, x._1)).distinct()
        uItem.sortBy(_._1).repartition(1).saveAsTextFile("/user/userCart/" + day + ".txt")
      }
      i = 0
      val day1 = sc.textFile("/user/userCart/" + timeData.apply(i) + ".txt", 3).map(x =>
        (x.split(",")(0), x.split(",")(1)))//.map((_, 1))
      val day2 = sc.textFile("/user/userCart/" + timeData.apply(i + 7) + ".txt", 3).map(x =>
        (x.split(",")(0), x.split(",")(1)))//.map((_, 1))
      val day3 = sc.textFile("/user/userCart/" + timeData.apply(i + 14) + ".txt", 3).map(x =>
        (x.split(",")(0), x.split(",")(1)))//.map((_, 1))
      val day4 = sc.textFile("/user/userCart/" + timeData.apply(i + 21) + ".txt", 3).map(x =>
        (x.split(",")(0), x.split(",")(1)))//.map((_, 1))
      val day5 = sc.textFile("/user/userCart/" + timeData.apply(i + 28) + ".txt", 3).map(x =>
        (x.split(",")(0), x.split(",")(1)))//.map((_, 1))
      val unionData = day1.union(day2).union(day3).union(day4).union(day5).distinct()
      unionData.repartition(1).saveAsTextFile("/user/userCart/unionData.txt")
      System.out.println(unionData.collect().length)

      // */
      sc.stop()
    }
}
