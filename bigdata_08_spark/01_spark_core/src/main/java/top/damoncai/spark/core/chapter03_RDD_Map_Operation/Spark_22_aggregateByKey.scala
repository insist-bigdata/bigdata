package top.damoncai.spark.core.chapter03_RDD_Map_Operation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_22_aggregateByKey {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // aggregateByKey 算子是函数柯里化，存在两个参数列表
    // 1. 第一个参数列表中的参数表示初始值
    // 2. 第二个参数列表中含有两个参数
    // 2.1 第一个参数表示分区内的计算规则
    // 2.2 第二个参数表示分区间的计算规则
    val rdd: RDD[(String, Int)] = sc.makeRDD(
      List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      )
    )
    // 0:("a",1),("a",2),("c",3) => (a,10)(c,10)
    //                                           => (a,10)(b,10)(c,20)
    // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)
    rdd.aggregateByKey(0)(
      (x,y) => math.max(x,y),
      (x,y) => x + y
    )
    sc.stop()
  }
}
