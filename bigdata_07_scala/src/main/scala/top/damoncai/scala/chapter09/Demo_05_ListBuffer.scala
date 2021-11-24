package top.damoncai.scala.chapter09

import scala.collection.mutable.ListBuffer

object Demo_05_ListBuffer {

  def main(args: Array[String]): Unit = {
    // 1.创建ListBuffer
    var list:ListBuffer[Int] = new ListBuffer[Int]();

    var list2 = ListBuffer.apply(1,2,3,4,5);

    // 2.遍历
    list2.foreach(println)

    // 3.添加元素
    list2 += 6
    println(list2)

    -1 +=: 0 +=: list2 += 7 += 8
    println(list2)

    list2.append(9,10)
    list2.prepend(-3,-2)

    // 4.两个集合相加
    var list3 = list ++ list2
    println(list3)

    // 4.修改集合元素
    list3(0) = 3
    println(list3)

    // 5.删除集合元素
    list3.remove(0)
    list3 -= 10
    println(list3)
  }
}
