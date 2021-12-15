package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_03_Map_Operator_Transfoem_Par {

  /**
   *
   * 同一个分区中执行时有序的
   * 不同分区之前是我发保证有序的
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    var list = List[Int](1,2,3,4,5)

    val value: RDD[Int] = sc.makeRDD(list,2)
    val map1: RDD[Int] = value.map(
      num => {
        println(">>>>>>>>>> " + num)
        num
      }
    )

    val map2: RDD[Int] = map1.map(
      num => {
        println("<<<<<<<<< " + num)
        num
      }
    )

    map2.collect()
    sc.stop()
  }
}
