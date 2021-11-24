package top.damoncai.scala.chapter09

import scala.::

object Demo_04_List {

  def main(args: Array[String]): Unit = {
    var list = List(1,2,3,4,5)

    // 1.添加元素 - 前面
    var list2 = list.::(6)
    println(list)
    println(list2)

    // 2.创建集合
    val list3 = Nil.::(11)
    println("list3: " + list3)

    val list4 = 1 :: 2 :: 3 :: 4 :: 5 :: Nil
    println("list4: " + list4)

    // 3.遍历
    list4.foreach(println)

    // 4.合并列表
    var list5 = list3 ++ list4
    var list6 = list3 ::: list4
    println("list5")
  }
}
