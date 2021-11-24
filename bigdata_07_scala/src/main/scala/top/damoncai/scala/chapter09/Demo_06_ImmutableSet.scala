package top.damoncai.scala.chapter09

import scala.collection.mutable.ListBuffer

object Demo_06_ImmutableSet {

  def main(args: Array[String]): Unit = {
    // 1.创建Set集合
    var set = Set(1,2,3,4,4,5,5);
    println(set)

    // 2.添加元素
    var set2 = set.+(6)
    println(set2)

    var set3 = set + 7
    println(set3)

    // 3.set集合合并
    var set4 = set2 ++ set3;
    println(set4)

    // 4.删除元素
    var set5 = set - 1
    println(set5)
  }
}
