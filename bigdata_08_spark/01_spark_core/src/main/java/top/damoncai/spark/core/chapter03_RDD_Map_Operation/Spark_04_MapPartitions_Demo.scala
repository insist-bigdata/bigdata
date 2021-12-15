package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_04_MapPartitions_Demo {

  /**
   * 获取每个分区的最大值
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    var list = List[Int](1,2,3,4,5)

    val value: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5),2)
    // 传递函数参数为分区的迭代器
    val map1: RDD[Int] = value.mapPartitions(
      iter =>{
        List(iter.max).iterator
      }
    )

    map1.collect().foreach(println)
    sc.stop()
  }
}
