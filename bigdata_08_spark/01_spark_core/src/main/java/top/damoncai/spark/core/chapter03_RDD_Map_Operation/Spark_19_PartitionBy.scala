package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark_19_PartitionBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4),1)

    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))
    mapRdd.partitionBy(new HashPartitioner(2))

    sc.stop()
  }
}
