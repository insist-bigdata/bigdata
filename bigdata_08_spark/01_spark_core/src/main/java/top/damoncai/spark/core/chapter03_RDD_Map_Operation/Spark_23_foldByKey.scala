package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_23_foldByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      )
    )

    val value: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)

    value.collect().foreach(println)

    sc.stop()
  }
}
