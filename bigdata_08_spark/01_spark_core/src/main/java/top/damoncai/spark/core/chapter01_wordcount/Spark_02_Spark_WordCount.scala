package top.damoncai.spark.core.chapter01_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_Spark_WordCount {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    //创建Spark上下文对象
    val sparkContext = new SparkContext(sparkConf)

    // 业务逻辑
    //1.读取文件数据
    val wordRDD:RDD[String] = sparkContext.textFile("bigdata_08_spark/datas")
    // 扁平化
    val wordFlatMapRDD: RDD[String] = wordRDD.flatMap(_.split(" "))
    // 每个元素打上 1
    val value: RDD[(String, Int)] = wordFlatMapRDD.map((_, 1))
    // Spark提供根据key 将value做reduce操作
    val res: RDD[(String, Int)] = value.reduceByKey(_ + _)
    // 打印
    res.foreach(println)
    //关闭资源
    sparkContext.stop()
  }
}
