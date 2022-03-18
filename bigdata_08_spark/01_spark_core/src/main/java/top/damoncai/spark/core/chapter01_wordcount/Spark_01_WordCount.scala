package top.damoncai.spark.core.chapter01_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_WordCount {

  def main(args: Array[String]): Unit = {
    // 创建Spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local").setAppName("wordcount")
    //创建Spark上下文对象
    val sparkContext = new SparkContext(sparkConf)

    // 业务逻辑
    //1.读取文件数据
    val wordRDD:RDD[String] = sparkContext.textFile("bigdata_08_spark/datas/demo/hello.txt")
    // 扁平化
    val wordFlatMapRDD: RDD[String] = wordRDD.flatMap(_.split(" "))
    // 分组
    val wordGroup: RDD[(String, Iterable[String])] = wordFlatMapRDD.groupBy(word => word)
    // 统计
    val wordGroupMap: RDD[(String, Int)] = wordGroup.map {case (word,count) => (word,count.size)}
    val tuples: Array[(String, Int)] = wordGroupMap.collect()
    tuples.foreach(println)

    //关闭资源
    sparkContext.stop()
  }

}
