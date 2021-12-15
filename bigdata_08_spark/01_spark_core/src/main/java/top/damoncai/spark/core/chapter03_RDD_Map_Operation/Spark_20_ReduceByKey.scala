package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark_20_ReduceByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 1)))

    val value: RDD[(String, Int)] = rdd.reduceByKey(_ + _)

    val value2: RDD[(String, Int)] = rdd.reduceByKey(_ + _,2)

    value.collect().foreach(println)

    value2.collect().foreach(println)

    sc.stop()
  }
}
