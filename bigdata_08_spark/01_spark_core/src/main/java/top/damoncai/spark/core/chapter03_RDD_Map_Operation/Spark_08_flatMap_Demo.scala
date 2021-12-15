package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_08_flatMap_Demo {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // 将不规则集合扁平化操作
    val list = List(
      List(1,2),
      3,
      List(4,5)
    )

    val rdd: RDD[Any] = sc.makeRDD(list)
    val value: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list:List[_] => list
          case dat => List(dat)
        }
      }
    )

    value.collect().foreach(println)
    sc.stop()
  }


}
