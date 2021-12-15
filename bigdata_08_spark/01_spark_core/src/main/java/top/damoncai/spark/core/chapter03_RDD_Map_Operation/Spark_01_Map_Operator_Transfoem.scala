package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_01_Map_Operator_Transfoem {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    var list = List[Int](1,2,3,4,5)

    val value: RDD[Int] = sc.makeRDD(list)

    // 使用匿名函数
    value.map(_*2).collect().foreach(println)

    println("=" * 10)

    // 传递函数的方式
    def fun(num:Int):Int = num * 3
    value.map(fun).collect().foreach(println)

    sc.stop()
  }
}
