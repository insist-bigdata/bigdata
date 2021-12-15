package top.damoncai.spark.core.chapter02_RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_02_File {

  def main(args: Array[String]): Unit = {

    // 从文件中获取

    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    // 相对路径是父工程
    val value: RDD[String] = sc.textFile("bigdata_08_spark/datas")

    value.collect().foreach(println);

    println("=" * 10)

    // 文件可以使用正则
    val value2: RDD[String] = sc.textFile("bigdata_08_spark/datas/*.txt")

    // 可以从hdfs中获取数据
    val value3: RDD[String] = sc.textFile("hdfs://ha01.prdigital.cn/input")

    value3.collect().foreach(println);

    // 关闭资源
    sc.stop()
  }

}
