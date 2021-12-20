package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_10_save {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String,Int)] = sc.makeRDD(
      List(
        ("a",1),
        ("a",2),
        ("a",3),
        ("b",1),
      )
    )

    // 保存成 Text 文件
    rdd.saveAsTextFile("bigdata_08_spark/datas/output")
    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("bigdata_08_spark/datas/output1")
    // 保存成 Sequencefile 文件
    rdd.saveAsSequenceFile("bigdata_08_spark/datas/output2")

    sc.stop()
  }
}
