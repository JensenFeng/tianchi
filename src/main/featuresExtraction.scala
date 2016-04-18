import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by jie on 4/13/16.
  * userid, itemid, behavior, geohash, category, time
  * item, geohash, category
  */
object featuresExtraction {
  def main(args: Array[String]): Unit ={
    if(args.length < 2){
      println("Usage args")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("features")
    val sc = new SparkContext(conf)
    //user.csv
    val rawUserData = sc.textFile(args(0)).map{
      line =>
        (line.split(",")(0), line.split(",")(1), line.split(",")(2).toInt, line.split(",")(5))
    }.cache()

    //Global
    val lastDay = "2014-12-18" //args()
    val lastDis = if(lastDay.split(" ")(0).split("-")(1).toInt >= 12){
        (lastDay.split(" ")(0).split("-")(2).toInt + 12)
      }else{
        (lastDay.split(" ")(0).split("-")(2).toInt - 18)
      }
    println("lastDay Dis : " + lastDis)
    //*counting
    //[(user,item),(sum1,sum2,sum3,sum4 - 1.1.1),(day1,day2,day3,day4 - 1.1.2), afterbuyclickcount - 1.1.3,
    // (Conversion ratio - 1.2.1), ((existAction) - 1.3.1), (twiceBuy - 1.3.2),
    // (firstDis, finalDis, finalBuyDis - 1.4.(1,2,3) )]
    val userGroup = rawUserData.map(data => ((data._1, data._2), data._3, data._4))
        .groupBy(_._1).map{
      case(k, v) =>
        val arr = v.seq.toList
          val sum1 = arr.count(_._2 == 1)
          val sum2 = arr.count(_._2 == 2)
          val sum3 = arr.count(_._2 == 3)
          val sum4 = arr.count(_._2 == 4)
          val exist1 = if(arr.count(_._2 == 1) > 0) true else false
          val exist2 = if(arr.count(_._2 == 2) > 0) true else false
          val exist3 = if(arr.count(_._2 == 3) > 0) true else false
          val exist4 = if(arr.count(_._2 == 4) > 0) true else false
        val day = v.seq.toList.distinct
          val day1 = day.count(_._2 == 1)
          val day2 = day.count(_._2 == 2)
          val day3 = day.count(_._2 == 3)
          val day4 = day.count(_._2 == 4)
        val sortarr = arr.sortBy(_._3)
        val clickcount = if(sortarr.indexWhere(_._2 == 4) != -1)
                            arr.length - 1 - sortarr.indexWhere(_._2 == 4)
                         else 0

        val arry = sortarr
        val firstDay = arry.apply(0)._3
        val finalDay = arry.last._3
        val firstDis = if(firstDay.split(" ")(0).split("-")(1).toInt >= 12){
          (firstDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (firstDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        val finalDis = if(finalDay.split(" ")(0).split("-")(1).toInt >= 12){
          (finalDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (finalDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        val finalBuyDay = {
          val t = arry.indexWhere(_._2 == 4)
          if(t != -1)
            arry.apply(t)._3
          else lastDay
        }
        val finalBuyDis = if(finalBuyDay.split(" ")(0).split("-")(1).toInt >= 12){
          (finalBuyDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (finalBuyDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        //1.3.2
        val twiceBuy = arr.filter(_._2 == 4)
        (k._1, (k._2, ((sum1, sum2, sum3, sum4), (day1, day2, day3, day4), clickcount,
          (("%.2f").format(sum4 * 1.0 / arr.length), ("%.2f").format(day4 * 1.0 / arr.length)),
          (exist1, exist2, exist3, exist4), twiceBuy.length >= 2),
          ((lastDis - firstDis, lastDis - finalDis), lastDis - finalBuyDis, finalDis - firstDis)))
    }.persist()
    userGroup.repartition(1).saveAsTextFile("/user/features/counting.txt")
    //1.2.3 [(user, action1, action2, action3, action4)]
    val userActionCount = rawUserData.map(data => (data._1, data._3)).groupBy(_._1).map{
      case(u, v) =>
        val arr = v.seq.toList
        (u, (arr.count(_._2 == 1), arr.count(_._2 == 2), arr.count(_._2 == 3), arr.count(_._2 == 4)))
    }.persist()
    //[(user,item),(sum1,sum2,sum3,sum4 - 1.1.1),(day1,day2,day3,day4 - 1.1.2), afterbuyclickcount - 1.1.3,
    // (Conversion ratio(c1, c2) - 1.2.1), ((existAction) - 1.3.1), (twiceBuy - 1.3.2), *(Cross ratio - 1.2.3),
    // (firstDis, finalDis, finalBuyDis - 1.4.(1,2,3) )]
    val userItemGroup = userGroup.join(userActionCount).map{
      case(u, ((p, item, dis), count)) =>
        val a1 = if(item._1._1 > 0) ("%.2f").format(count._1 * 1.0 / item._1._1) else -1
        val a2 = if(item._1._2 > 0) ("%.2f").format(count._2 * 1.0 / item._1._2) else -1
        val a3 = if(item._1._3 > 0) ("%.2f").format(count._3 * 1.0 / item._1._3) else -1
        val a4 = if(item._1._4 > 0) ("%.2f").format(count._4 * 1.0 / item._1._4) else -1
        ((u, p), item, (a1, a2, a3, a4), dis)
    }
    userItemGroup.repartition(1).saveAsTextFile("/user/features/useritemgroup.csv")
    // */
    //*
    //(user, (action:product - 1.1.4), (r1,r2 - 1.2.1), (1.2.2), (1.3.1), (1.3.2), (1.4.1, 1.4.2, 1.4.3))
    val userAction = rawUserData.distinct().groupBy(_._1).map{
      case(k, v) =>
        val a1 = v.seq.count(_._3 == 1)
        val a2 = v.seq.count(_._3 == 2)
        val a3 = v.seq.count(_._3 == 3)
        val a4 = v.seq.count(_._3 == 4)
        val exist1 = if(a1 > 0) true else false
        val exist2 = if(a2 > 0) true else false
        val exist3 = if(a3 > 0) true else false
        val exist4 = if(a4 > 0) true else false
        val day = v.seq.toList.map(x => (x._3, x._4)).distinct
          val day1 = day.count(_._1 == 1)
          val day2 = day.count(_._1 == 2)
          val day3 = day.count(_._1 == 3)
          val day4 = day.count(_._1 == 4)
        val d1 = if(day1 > 0) ("%.2f").format(a1 * 1.0 / day1) else -1
        val d2 = if(day2 > 0) ("%.2f").format(a2 * 1.0 / day2) else -1
        val d3 = if(day3 > 0) ("%.2f").format(a3 * 1.0 / day3) else -1
        val d4 = if(day4 > 0) ("%.2f").format(a4 * 1.0 / day4) else -1
        val len = v.seq.toList.length

        val arry = v.seq.toList.sortBy(_._4)
        val firstDay = arry.apply(0)._4
        val finalDay = arry.last._4
        val firstDis = if(firstDay.split(" ")(0).split("-")(1).toInt >= 12){
          (firstDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (firstDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        val finalDis = if(finalDay.split(" ")(0).split("-")(1).toInt >= 12){
          (finalDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (finalDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        val finalBuyDay = {
          val t = arry.indexWhere(_._3 == 4)
          if(t != -1)
            arry.apply(t)._4
          else lastDay
        }
        val finalBuyDis = if(finalBuyDay.split(" ")(0).split("-")(1).toInt >= 12){
          (finalBuyDay.split(" ")(0).split("-")(2).toInt + 12)
        }else{
          (finalBuyDay.split(" ")(0).split("-")(2).toInt - 18)
        }
        val twiceBuy = v.seq.toList.filter(_._3 == 4).distinct
        (k, (a1, a2, a3, a4), (("%.2f").format(a4 * 1.0 / len), ("%.2f").format(day4 * 1.0 / len)),
          (d1, d2, d3, d4), (exist1, exist2, exist3, exist4), (twiceBuy.length >= 2),
          ((lastDis - firstDis, lastDis - finalDis), lastDis - finalBuyDis, finalDis - firstDis))
    }
    userAction.repartition(1).saveAsTextFile("/user/features/useraction.csv")
    // */
    //[(product, (action:user - 1.1.4), (r1,r2 - 1.2.1), (1.2.2), (1.3.1), (1.3.2), ((buyuser.length >= 2 else -1)- 1.4.4)]
    val productAction = rawUserData.distinct().groupBy(_._2).map{
      case(k, v) =>
        val a1 = v.seq.count(_._3 == 1)
        val a2 = v.seq.count(_._3 == 2)
        val a3 = v.seq.count(_._3 == 3)
        val a4 = v.seq.count(_._3 == 4)
        val exist1 = if(a1 > 0) true else false
        val exist2 = if(a2 > 0) true else false
        val exist3 = if(a3 > 0) true else false
        val exist4 = if(a4 > 0) true else false
        val day = v.seq.toList.map(x => (x._3, x._4)).distinct
        val day1 = day.count(_._1 == 1)
        val day2 = day.count(_._1 == 2)
        val day3 = day.count(_._1 == 3)
        val day4 = day.count(_._1 == 4)
        val d1 = if(day1 > 0) ("%.2f").format(a1 * 1.0 / day1) else -1
        val d2 = if(day2 > 0) ("%.2f").format(a2 * 1.0 / day2) else -1
        val d3 = if(day3 > 0) ("%.2f").format(a3 * 1.0 / day3) else -1
        val d4 = if(day4 > 0) ("%.2f").format(a4 * 1.0 / day4) else -1
        val len = v.seq.toList.length
        val buyuser = v.seq.toList.filter(_._3 == 4).map(x => x._1).distinct
        val buyuserlen = if(buyuser.length >= 2) buyuser.length else -1
        val twiceBuy = v.seq.toList.filter(_._3 == 4).distinct
        (k, (a1, a2, a3, a4), (("%.2f").format(a4 * 1.0 / len), ("%.2f").format(day4 * 1.0 / len)),
          (d1, d2, d3, d4), (exist1, exist2, exist3, exist4), (twiceBuy.length >= 2), buyuserlen)
    }
    productAction.repartition(1).saveAsTextFile("/user/features/productaction.csv")
    // */


    /* 暂时不用
   val actionData = rawUserData.map(data => (data._1, data._3)).distinct().cache()
   (1 to 4).foreach{
     i =>
       val ac = actionData.filter(_._2 == i)
       ac.repartition(1).saveAsTextFile("/user/features/useraction" + i + "_" + ac.count + ".txt")
   }
   val productAction = rawUserData.map(data => (data._2, data._3)).distinct().cache()
   (1 to 4).foreach{
     i =>
       val ac = productAction.filter(_._2 == i)
       ac.repartition(1).saveAsTextFile("/user/features/productaction" + i + "_" + ac.count + ".txt")
   }
   // */
    sc.stop()
  }
}
