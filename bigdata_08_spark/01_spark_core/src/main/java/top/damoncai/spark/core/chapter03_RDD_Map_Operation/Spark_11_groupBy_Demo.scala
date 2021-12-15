package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_11_groupBy_Demo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    /**
     * 从服务器日志数据 apache.log 中获取每个时间段访问量。
     * 83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
     */
    val rdd: RDD[String] = sc.textFile("bigdata_08_spark/datas/apache.log")
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/DD:HH:mm:ss")

    val mapRDD: RDD[(Int, Int)] = rdd.map(
      line => {
        var time = line.split(" ")(3)
        val hours: Int = sdf.parse(time).getHours
        (hours, 1)
      }
    )

    val groupRDD: RDD[(Int, Iterable[(Int, Int)])] = mapRDD.groupBy(_._1)
    val resRDD: RDD[(Int, Int)] = groupRDD.map({
      case (hour, iter) => (hour, iter.size)
    })

    resRDD.collect().foreach(println)

    sc.stop()
  }
}
