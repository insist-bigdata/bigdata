package top.damoncai.scala.chapter09

import scala.collection.mutable

object Demo_09_MutableMap {

  def main(args: Array[String]): Unit = {
    // 1.创建Map
    var map:mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2);
    println(map)
    println(map.getClass)

    println("======================")
    // 2.遍历
    map.foreach(println)

    println("======================")
    // 3.添加元素
    map.put("c",3)
    map += (("d",4))
    println(map)

    println("======================")
    // 4.删除元素
    map.remove("a")
    map -=("b")
    println(map)

    println("======================")
    // 5.获取key集合
    for (key <- map.keySet) {
      println(s"$key --> ${map.get(key)}")
    }

    println("======================")
    // 6.获取某个元素
    var numa = map.getOrElse("a",0);
    var numb = map.getOrElse("c",0);
    println(numa)
    println(numb)

    var num = map("c")
    println(num)
  }
}
