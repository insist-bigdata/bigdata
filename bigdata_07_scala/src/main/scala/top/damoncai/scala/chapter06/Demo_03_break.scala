package top.damoncai.scala.chapter06

import scala.util.control.Breaks

object Demo_03_break {


  def main(args: Array[String]): Unit = {
    // 循环中断 - 异常
    try {
      for (i <- 1 to 5 by 1) {
        if(i == 3) throw new RuntimeException
        println(i)
      }
    }catch {
      case e =>
    }

    // 循环中断 - scala自带函数
    Breaks.breakable(
      for (i <- 1 to 5 by 1) {
        if(i == 3) Breaks.break();
        println(i)
      }
    )
  }
}
