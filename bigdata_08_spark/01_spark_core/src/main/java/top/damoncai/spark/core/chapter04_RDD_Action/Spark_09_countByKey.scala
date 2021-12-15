package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_09_countByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String,Int)] = sc.makeRDD(
      List(
        ("a",1),
        ("a",2),
        ("a",3),
        ("b",1),
      )
    )

    val res: collection.Map[String, Long] = rdd.countByKey()

    println("res：" + res)

    sc.stop()
  }
}
