package top.damoncai.spark.core.chapter10_Case

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}
import top.damoncai.spark.core.chapter08_Acc.Spark_02_Customer_Acc.MyAcc

import scala.collection.mutable

object Spark_01_Case_03 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Case_01")
    val sc = new SparkContext(sparkConf)

    val acc = new MyAcc()

    sc.register(acc)
    /**
     * 先按照点击数排名，靠前的就排名高；如果点击数相同，再比较下单数；下单数再相同，就比较支付数。
     */

    val rdd: RDD[String] = sc.textFile("bigdata_08_spark/datas/user_visit_action.txt")
    val flaMapRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap(line => {
      val arr: Array[String] = line.split("_")
      if (arr(6) != "-1") {
        acc.add((arr(6),"point"))
        List((arr(6), (1, 0, 0)))
      } else if (arr(8) != "null") {
        val orders: Array[String] = arr(8).split(",")
        orders.map(item => {
          acc.add((arr(6),"order"))
          (item, (0, 1, 0))
        })
      } else if (arr(11) != "null") {
        val pays: Array[String] = arr(11).split(",")
        pays.map(item => {
          acc.add((arr(6),"pay"))
          (item, (0, 0, 1))
        })
      } else {
        Nil
      }
    })

    for (elem <- flaMapRDD.take(10)) {
      println(elem)
    }
    sc.stop()
  }

  case class OneAction(var point:Int, order:Int,var pay:Int)

  class MyAcc extends AccumulatorV2[(String,String),mutable.Map[String,OneAction]] {

    /**
     * 定义变量接收数据
     */
    private val wcMap: mutable.Map[String, OneAction] = mutable.Map[String, OneAction]()

    /**
     * 判断是否为初始状态
     * @return
     */
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String,String), mutable.Map[String,OneAction]] = {
      new MyAcc()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    override def merge(other: AccumulatorV2[(String,String), mutable.Map[String, OneAction]]): Unit = {
      other.value.foreach(item => {
        item match {
          case (id,oneAction) => {
            val action: OneAction = wcMap.getOrElse(id, OneAction(0, 0, 0))
            action.point += oneAction.point
            action.order += oneAction.order
            action.pay += oneAction.pay
            wcMap.update(id,action)

          }
        }
      })
    }

    override def add(v: (String, String)): Unit = {
      v match {
        case (id,ac) => {
          val action: OneAction = wcMap.getOrElse(id, OneAction(0, 0, 0))
          if(ac == "point") {
            action.point += 1
          }else if(ac == "order") {
            val action: OneAction = wcMap.getOrElse(id, OneAction(0, 0, 0))
            action.order += 1
          }else if(ac == "pay") {
            val action: OneAction = wcMap.getOrElse(id, OneAction(0, 0, 0))
            action.pay += 1
          }
          wcMap.update(id,action)
        }
      }
    }

    override def value: mutable.Map[String, OneAction] = {
      wcMap
    }
  }
}