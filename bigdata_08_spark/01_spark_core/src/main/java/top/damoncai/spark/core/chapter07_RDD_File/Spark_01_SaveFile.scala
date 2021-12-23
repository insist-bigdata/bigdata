package top.damoncai.spark.core.chapter07_RDD_File

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark_01_SaveFile {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String,String)] = sc.makeRDD(List(("nba"," XX"), ("cba"," XX"), ("lpl"," XX")))
    rdd.saveAsTextFile("bigdata_08_spark/datas/textfile")
    rdd.saveAsObjectFile("bigdata_08_spark/datas/objectfile")
    rdd.saveAsSequenceFile("bigdata_08_spark/datas/sequencefile")
  }
}