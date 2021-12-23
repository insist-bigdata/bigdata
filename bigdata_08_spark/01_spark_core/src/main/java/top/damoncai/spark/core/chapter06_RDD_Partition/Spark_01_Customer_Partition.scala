package top.damoncai.spark.core.chapter06_RDD_Partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark_01_Customer_Partition {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String,String)] = sc.makeRDD(List(("nba"," XX"), ("cba"," XX"), ("lpl"," XX")), 3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("bigdata_08_spark/datas/mypar")
    sc.stop()
  }

  class MyPartitioner extends Partitioner {

    override def numPartitions: Int = 3

    // 分区索引 从0开始
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }
}