package top.damoncai.spark.core.chapter10_Case

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Case_02 {

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
    val pointRdd: RDD[(String, (Int, Int, Int))] = baseRDD.filter(_._1 != "-1").map(item => (item._1, 1))
      .reduceByKey(_ + _)
      .map(item => {
        (item._1, (item._2, 0, 0))
      })
    // 下单数
    val orderRDD: RDD[(String, (Int, Int, Int))] = baseRDD.filter(_._3 != "null").flatMap(item => {
      val arr: Array[String] = item._3.split(",")
      arr.map((_, 1))
    }).reduceByKey(_ + _)
      .map(item => {
        (item._1, (0, item._2, 0))
      })

    // 支付数
    val payRdd:RDD[(String, (Int, Int, Int))] = baseRDD.filter(_._4 != "null").flatMap(item => {
      val arr: Array[String] = item._4.split(",")
      arr.map((_, 1))
    }).reduceByKey(_+_)
      .map(item => {
        (item._1, ( 0, 0,item._2))
      })

    //组合数据
    val unionRDD: RDD[(String, (Int, Int, Int))] = pointRdd.union(orderRDD).union(payRdd)
    val res: Array[(String, (Int, Int, Int))] = unionRDD.reduceByKey((e1, e2) => {
      (
        e1._1 + e2._1, e1._2 + e2._2, e1._3 + e2._3
      )
    }).sortBy(_._2,false).take(10)

    res.foreach(println)
    sc.stop()
  }
}