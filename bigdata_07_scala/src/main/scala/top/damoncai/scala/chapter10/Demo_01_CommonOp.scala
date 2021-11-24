package top.damoncai.scala.chapter10

object Demo_01_CommonOp {

  def main(args: Array[String]): Unit = {
    // 1.获取集合长度和集合大小
    val list = List(1,2,3,4,5)
    var set = Set(1,2,3,4,5)
    println(list.length)
    println(list.size)
    println(set.size)

    println("=======================")
    for (ele <- list) println(ele)
    for (ele <- set) println(ele)

    println("=======================")
    list.foreach(println)
    set.foreach(println)

    println("=======================")
    for (ele <- list.iterator) println(ele)
    for (ele <- set.iterator) println(ele)

    println("=======================")
    println(list.mkString("-"))
    println(set.mkString("-"))

    println("=======================")
    println(list.contains(5))
    println(set.contains(5))

  }
}
