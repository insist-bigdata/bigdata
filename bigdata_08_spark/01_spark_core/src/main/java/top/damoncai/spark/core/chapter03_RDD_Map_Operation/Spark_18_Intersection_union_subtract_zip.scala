package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_18_Intersection_union_subtract_zip {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6))

    // 交集
    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    // 并集
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    // 差集
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    // 拉链 如果两个集合数据长度不一致，则报错
    //     如果两个集合分区数不一致，则报错
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    sc.stop()
  }
}
