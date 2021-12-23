package top.damoncai.spark.core.chapter07_RDD_File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_LoadFile {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rddText: RDD[String] = sc.textFile("bigdata_08_spark/datas/textfile")
    val rddObj: RDD[(String, String)] = sc.objectFile[(String, String)]("bigdata_08_spark/datas/objectfile")
    val rddSeq: RDD[(String, String)] = sc.sequenceFile[String, String]("bigdata_08_spark/datas/sequencefile")
    rddText.collect().foreach(println)
    println("+" * 10)
    rddObj.collect().foreach(println)
    println("+" * 10)
    rddSeq.collect().foreach(println)
  }
}