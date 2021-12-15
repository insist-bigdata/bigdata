package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_29_sortByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list = List((12,"a"),(8,"b"),(9,"c"),(6,"d"))

    val rdd: RDD[(Int, String)] = sc.makeRDD(list)
    val res: RDD[(Int, String)] = rdd.sortByKey(true) // 升序
    val res2: RDD[(Int, String)] = rdd.sortByKey(false) // 降序

    res.collect().foreach(println)

    sc.stop()
  }
}
