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
    val baseRDD: RDD[(String, String, String)] = rdd.map(line => {
      val arr: Array[String] = line.split("_")
      (arr(6), arr(8), arr(11))
    })
    // 点击数
    val tuples: Array[(String, Int)] = baseRDD.map(item => (item._1, 1)).filter(_._1 != "NULL").reduceByKey(_ + _).sortBy(_._2,false).take(10)
    tuples.foreach(println)
    sc.stop()
  }
}