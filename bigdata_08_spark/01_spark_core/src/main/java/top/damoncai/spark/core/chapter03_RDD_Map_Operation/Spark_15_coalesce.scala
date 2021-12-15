package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_15_coalesce {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)

    //会导致数据倾斜
    coalesceRDD.saveAsTextFile("bigdata_08_spark/datas/output")

    // 采用shuffer避免
    val coalesceRDD2: RDD[Int] = rdd.coalesce(2,true)

    coalesceRDD2.saveAsTextFile("bigdata_08_spark/datas/output2")



    sc.stop()
  }
}
