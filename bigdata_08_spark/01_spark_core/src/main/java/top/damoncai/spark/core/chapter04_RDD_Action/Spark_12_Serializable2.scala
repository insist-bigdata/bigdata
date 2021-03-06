package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_12_Serializable2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val user = new User("user", 3)
    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark","hive", "atguigu"))

    val search = new Search("hello")

    //3.2 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)
    //3.3 属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }
}

class Search(query:String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }
  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch)
  }
  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query))
    //val q = query
    //rdd.filter(x => x.contains(q))
  }
}

