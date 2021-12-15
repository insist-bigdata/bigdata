package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_25_combineByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 计算平均数

    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      )
    )

    val value: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v,1)
      ,
      (t1:(Int,Int), t2) => {
        val num: Int = t1._1 + 1 //次数
        val sum: Int = t1._2 + t2 // 和
        (num, sum)
      },
      (t1:(Int,Int), t2:(Int,Int)) => {
        val num: Int = t1._1 + t2._1
        val sum: Int = t1._2 + t2._2
        (num, sum)
      }
    )

    val res: RDD[(String, Int)] = value.mapValues({
      case (x, y) => y / x
    })

    res.collect().foreach(println)

    sc.stop()
  }
}
