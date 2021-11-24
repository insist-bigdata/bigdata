package top.damoncai.scala.chapter11

object Demo_04_MatchList {
  def main(args: Array[String]): Unit = {
    // 匹配列表
    for (arr <- List(
      List(0),
      List(1, 0),
      List(0, 2, 0),
      List(1, 1, 0),
      List(1, 1, 0, 1),
      List("hello", 90)
    )) {
      val result = arr match {
        case List(0) => "0" //匹配 Array(0) 这个列表
        case List(x, y) => x + "," + y //匹配有两个元素的列表，然后将将元素值赋给对应的 x,y
        case List(x,1,y) => "中间元素为1"
        case List(0, _*) => "以 0 开头的列表" //匹配以 0 开头和列表
        case _ => "something else"
      }
      println("result = " + result)
    }

    var list = List(1,2,3,4,5,6)
    var list2 = List(1,2)
    var list3 = List(1)

    list match {
      case first :: second :: rest => println(s"first: $first second: $second rest $rest")
      case _ => "something else"
    }

    list2 match {
      case first :: second :: rest => println(s"first: $first second: $second rest $rest")
      case _ => "something else"
    }

    list3 match {
      case first :: second :: rest => println(s"first: $first second: $second rest $rest")
      case _ => println("something else")
    }

  }
}
