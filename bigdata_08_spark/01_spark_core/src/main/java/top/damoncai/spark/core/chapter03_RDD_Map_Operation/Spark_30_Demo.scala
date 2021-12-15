package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_30_Demo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**
     * agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
     * 统计出每一个省份每个广告被点击数量排行的 Top3
     */
    val rdd: RDD[String] = sc.textFile("bigdata_08_spark/datas/agent.log")

    val mapRdd: RDD[((String, String), Int)] = rdd.map(
      record => {
        val arr = record.split(" ")
        ((arr(1), arr(4)), 1)
      }
    )
    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    val reduceMapRdd: RDD[(String, (String, Int))] = reduceRdd.map({
      case ((s, g), sum) => (s, (g, sum))
    })

    val groupReduceRdd: RDD[(String, Iterable[(String, Int)])] = reduceMapRdd.groupByKey()

    val res: RDD[(String, List[(String, Int)])] = groupReduceRdd.mapValues({
      case iter => {
        val tuples: List[(String, Int)] = iter.toList.sortBy(_._2)(Ordering[Int].reverse).take(3)
        tuples
      }
    })


    res.collect().foreach(println)

    sc.stop()
  }
}
