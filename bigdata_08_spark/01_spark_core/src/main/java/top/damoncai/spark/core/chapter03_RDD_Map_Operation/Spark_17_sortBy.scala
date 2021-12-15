package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_17_sortBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(6, 2, 4, 3, 5, 6), 2)
    val value: RDD[Int] = rdd.sortBy(num => num)

    // 第二个参数传递false，降序排序
    val value2: RDD[Int] = rdd.sortBy(num => num,false)
    value2.collect().foreach(println)

    sc.stop()
  }
}
