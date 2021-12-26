package top.damoncai.spark.core.chapter08_Acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_02_Customer_Acc {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val acc = new MyAcc()


    sc.register(acc)

    val rdd: RDD[String] = sc.makeRDD(List("hello","java", "hello","spark"))
    rdd.foreach(item => {
      // 使用累加器
      acc.add(item)
    })
    println("sum:" + acc.value)
  }

  /**
   * 1.继承AccumulatorV2并定义泛型
   *  泛型1：IN 输入
   *  泛型2：OUT 输出
   *
   */
  class MyAcc extends AccumulatorV2[String,mutable.Map[String,Int]] {

    /**
     * 定义变量接收数据
     */
    private var wcMap = mutable.Map[String,Int]()

    /**
     * 判断是否为初始状态
     * @return
     */
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
      new MyAcc()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    /**
     * 添加操作
     * @param word
     */
    override def add(word: String): Unit = {
      wcMap.put(word,wcMap.getOrElse(word,0) + 1);
    }

    /**
     * 合并
     * @param other
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      other.value.map( e =>
        e match {
          case (k,v) => {
            wcMap.update(k,wcMap.getOrElse(k,0) + v)
          }
        }
      )
    }

    /**
     * 累加器结果
     * @return
     */
    override def value: mutable.Map[String, Int] = {
      wcMap
    }
  }
}