package top.damoncai.spark.core.chapter10_Case

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Case_01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Case_01")
    val sc = new SparkContext(sparkConf)
    /**
     * 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
     */

    val rdd: RDD[String] = sc.textFile("bigdata_08_spark/datas/user_visit_action.txt")
    val baseRDD: RDD[(String, String,String, String)] = rdd.map(line => {
      val arr: Array[String] = line.split("_")
      (arr(6),arr(7), arr(8), arr(11))
    })
    // 点击数
    val pointRdd: RDD[(String, Int)] = baseRDD.filter(_._1 != "-1").map(item => (item._1, 1)).reduceByKey(_ + _)
    // 下单数
    val orderRdd: RDD[(String, Int)] = baseRDD.filter(_._3 != "null").flatMap(item => {
      val arr: Array[String] = item._3.split(",")
      arr.map((_, 1))
    }).reduceByKey(_+_)
    // 支付数
    val payRdd: RDD[(String, Int)] = baseRDD.filter(_._4 != "null").flatMap(item => {
      val arr: Array[String] = item._4.split(",")
      arr.map((_, 1))
    }).reduceByKey(_+_)
    //组合数据
    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = pointRdd.cogroup(orderRdd, payRdd)
    val res: Array[(String, (Int, Int, Int))] = cogroupRdd.mapValues(item =>
      item match {
        case (it1, it2, it3) => {
          (
            if (it1.iterator.hasNext) it1.iterator.next() else 0,
            if (it2.iterator.hasNext) it2.iterator.next() else 0,
            if (it3.iterator.hasNext) it3.iterator.next() else 0
          )
        }
      }
    ).sortBy(_._2, false).take(10)

    res.foreach(println)
    sc.stop()
  }
}