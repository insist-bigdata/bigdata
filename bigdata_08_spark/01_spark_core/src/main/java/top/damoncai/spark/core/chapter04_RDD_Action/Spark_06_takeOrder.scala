package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_06_takeOrder {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4, 5), 2)
    val arr: Array[Int] = rdd.takeOrdered(3);

    println("res：" + arr.mkString("-"))

    sc.stop()
  }
}
