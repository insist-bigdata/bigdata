package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_26_join {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val list1 = List(("a",1),("a",2),("b",3),("c",4))
    val list2 = List(("a",11),("a",22),("b",33),("d",55))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(list1)
    val rdd2: RDD[(String, Int)] = sc.makeRDD(list2)

    // 笛卡尔积（内连接）但需key相等
    val res: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    res.collect().foreach(println)

    sc.stop()
  }
}
