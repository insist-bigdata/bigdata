package top.damoncai.scala.chapter09

import scala.collection.mutable

object Demo_10_Tuple {

  def main(args: Array[String]): Unit = {
    // 1.创建元组
    var tuple:(String,Int) = ("张三",12);
    println(tuple)

    println("======================")
    // 2.访问
    println(tuple._1)
    println(tuple._2)

    println(tuple.productElement(1)) // 通过索引访问数据

    println("======================")
    // 3.遍历
    for (ele <- tuple.productIterator) println(ele)

    println("======================")
    // 4.创建元组嵌套
    var tuple2 = ("张三",12,("pp","basektball"));
    println(tuple2._3._2)
  }
}
