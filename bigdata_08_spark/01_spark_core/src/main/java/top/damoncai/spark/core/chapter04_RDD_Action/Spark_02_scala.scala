package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_scala {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)
    val res: Int = rdd.reduce((v1, v2) => {
      println(s"vl:$v1,v2:$v2")
      v1 + v2
    })

    println("res：" + res)

    sc.stop()
  }
}
