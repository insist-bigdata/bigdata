package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_Map_Operator_Transfoem_Demo {

  /**
   * 输出日志文件中的地址
   * @param args
   */

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val value: RDD[String] = sc.textFile("bigdata_08_spark/datas/apache.log")

    value.map(_.split(" ")(6)).collect().foreach(println)

    sc.stop()
  }
}
