package top.damoncai.scala.chapter09

import scala.collection.mutable

object Demo_07_MutableSet {

  def main(args: Array[String]): Unit = {
    // 1.创建Set集合
    var set = mutable.Set(1,2,3,4,4,5,5);
    println(set)

    // 2.添加元素
    set += 6
    println(set)

    set.add(7)
    println(set)

    var set2 = set.+(8) // 向集合中添加元素，返回一个新的 Set
    println(set2)

    // 3.set集合合并
    var set3 = set ++ set2
    println(set3)

    set ++= set3
    println(set)

    // 4.删除元素
    set -= 8
    println(set)
  }
}
