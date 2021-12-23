package top.damoncai.spark.core.chapter05_RDD_Persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark_03_Persist {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("hello java","hello spark","spark sql"))
    val rddFlatMap = rdd.flatMap(_.split(" "))
    val rddMap: RDD[(String, Int)] = rddFlatMap.map(item => {
      println("@@@")
      (item, 1)
    })
    rddMap.persist(StorageLevel.DISK_ONLY_2)
    val rddReduceByKey: RDD[(String, Int)] = rddMap.reduceByKey(_ + _)
    rddReduceByKey.collect().foreach(println)

    println("++++++++++++++++++++++++++++++")
    val groupRdd: RDD[(String, Iterable[Int])] = rddMap.groupByKey()
    groupRdd.collect().foreach(println)
    sc.stop()
  }
}
