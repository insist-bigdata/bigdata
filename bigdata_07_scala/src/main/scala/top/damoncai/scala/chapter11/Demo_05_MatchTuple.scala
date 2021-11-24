package top.damoncai.scala.chapter11

object Demo_05_MatchTuple {
  def main(args: Array[String]): Unit = {
    // 匹配数组
    for (arr <- List(
      (0),
      (1, 0),
      (0, 2, 0),
      (1, 1, 0),
      (1, 1, 0, 1),
      ("hello", 90)
    )) {
      val result = arr match {
        case (0) => "0" //匹配 (0) 这个元组
        case (x, y) => x + "," + y //匹配有两个元素的元组，然后将将元素值赋给对应的 x,y
        case (x,1,y) => "中间元素为1"
        case (0, _) => "以 0 开头的,并且第二个数任意数" //匹配以 0 开头和元组
        case _ => "something else"
      }
      println("result = " + result)
    }

    println("=================")
  }
}
