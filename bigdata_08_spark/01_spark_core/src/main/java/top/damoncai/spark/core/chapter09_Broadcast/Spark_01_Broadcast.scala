package top.damoncai.spark.core.chapter09_Broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Broadcast {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val broadcast: Broadcast[List[Int]] = sc.broadcast(List(1, 2, 3, 4, 5))

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

    val res: RDD[Int] = rdd.map(num => {
      val value: List[Int] = broadcast.value
      value.indexOf(num)
    })
    res.collect().foreach(println)
  }
}