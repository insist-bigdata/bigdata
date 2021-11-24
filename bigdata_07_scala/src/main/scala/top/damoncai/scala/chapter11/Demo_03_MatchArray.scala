package top.damoncai.scala.chapter11

object Demo_03_MatchArray {
  def main(args: Array[String]): Unit = {
    // 匹配数组
    for (arr <- List(
      Array(0),
      Array(1, 0),
      Array(0, 2, 0),
      Array(1, 1, 0),
      Array(1, 1, 0, 1),
      Array("hello", 90)
    )) {
      val result = arr match {
        case Array(0) => "0" //匹配 Array(0) 这个数组
        case Array(x, y) => x + "," + y //匹配有两个元素的数组，然后将将元素值赋给对应的 x,y
        case Array(x,1,y) => "中间元素为1"
        case Array(0, _*) => "以 0 开头的数组" //匹配以 0 开头和数组
        case _ => "something else"
      }
      println("result = " + result)
    }
  }
}
