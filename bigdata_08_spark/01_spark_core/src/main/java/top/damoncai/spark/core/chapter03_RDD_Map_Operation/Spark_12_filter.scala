package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_12_filter {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**
     * 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
     * 83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
     */
    val rdd: RDD[String] = sc.textFile("bigdata_08_spark/datas/apache.log")
    val filterDate = "17/05/2015";
    val filterRDD: RDD[String] = rdd.filter(
      line => {
        val dateStr = line.split(" ")(3)
        dateStr.startsWith(filterDate)
      }
    )

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}
