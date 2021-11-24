package top.damoncai.scala.chapter11

object Demo_01_Match {
  def main(args: Array[String]): Unit = {
    var num:Int = 2;

    var y = num match {
      case 1 => "one"
      case 2 => "two"
      case 3 => "three"
      case _ => "other"
    }
    println(y)

    def abs(num:Int):Int = {
      num match {
        case i if i >= 0 => i
        case i if i < 0 => -i
      }
    }

    println(abs(3))
    println(abs(-3))
  }
}
