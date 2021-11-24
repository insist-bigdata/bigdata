package top.damoncai.scala.chapter07

object Demo_07_ClosureAndCurrying {

  def main(args: Array[String]): Unit = {
    // 1. 闭包

    def funF(x:Int):Int =>Int =  x + _

    println(funF(4)(5));

    // 2.柯里化
    def funK(x:Int)(y:Int):Int = x + y
    println(funK(3)(7))
  }
}
