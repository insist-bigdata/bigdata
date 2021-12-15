package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_05_MapPartitions_Operator_Transfoem {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    var list = List[Int](1,2,3,4,5)

    val value: RDD[Int] = sc.makeRDD(List[Int](1,2,3,4,5),2)
    // 传递函数参数为分区的迭代器
    val map1: RDD[Int] = value.mapPartitions(
      iter =>{
        println(">>>>>>")
        iter.map(_*2)
      }
    )

    map1.collect()
    sc.stop()
  }
}
