package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_06_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5))

    val map1: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map((index, _))
      }
    )

    map1.collect().foreach(println)
    sc.stop()
  }
}
