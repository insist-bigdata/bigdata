package top.damoncai.spark.core.chapter04_RDD_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_12_Serializable {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val user = new User("user", 3)
    val rdd: RDD[User] = sc.makeRDD(
      List(new User("张三", 3), new User("李四", 4)), 2
    )

    rdd.foreach(e => println(user.name))

    sc.stop()
  }
}

class User extends Serializable {

  var name:String = _;

  var age:Int = _;

  def this(name:String,age:Int) {
    this()
    this.name = name
    this.age = age
  }
}
