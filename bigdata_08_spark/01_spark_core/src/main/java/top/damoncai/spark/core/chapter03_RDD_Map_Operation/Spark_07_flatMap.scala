package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_07_flatMap {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list:List[List[Int]] = List(
      List(1,2),
      List(3),
      List(4,5)
    )

    val rdd: RDD[List[Int]] = sc.makeRDD(list)
    val value: RDD[Int] = rdd.flatMap(
      list => list
    )

    value.collect().foreach(println)
    sc.stop()
  }


}
