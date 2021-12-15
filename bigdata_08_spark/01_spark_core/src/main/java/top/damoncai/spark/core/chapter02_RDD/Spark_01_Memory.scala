package top.damoncai.spark.core.chapter02_RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Memory {

  def main(args: Array[String]): Unit = {

    // 从内存中读取数据
    val list = List(1,2,3,4,5)

    // 创建Spark运行配置对象
    // [*] 标识根据当前计算机的合数进行模拟
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    //创建Spark上下文对象
    val sc = new SparkContext(sparkConf)

    val value1: RDD[Int] = sc.parallelize(list)

    value1.collect().foreach(println);

    println("=" * 10)

    val value2: RDD[Int] = sc.makeRDD(list)

    value2.collect().foreach(println);

    // 关闭资源
    sc.stop()
  }

}
