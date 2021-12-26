package top.damoncai.spark.core.chapter08_Acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Acc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val sum: LongAccumulator = sc.longAccumulator("sum")
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))
    rdd.foreach(num => {
      // 使用累加器
      sum.add(num)
    })
    println("sum:" + sum.value)
  }
}