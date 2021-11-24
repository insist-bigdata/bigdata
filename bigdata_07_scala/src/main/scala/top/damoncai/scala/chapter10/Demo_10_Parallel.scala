package top.damoncai.scala.chapter10

object Demo_10_Parallel {
  def main(args: Array[String]): Unit = {
    var list = (1 to 100).map(x => Thread.currentThread().getId)
    println(list)

    var list2 = (1 to 100).par.map(x => Thread.currentThread().getId)
    println(list2)
  }
}
