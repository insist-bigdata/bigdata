package top.damoncai.scala.chapter12

object Demo_01_Exception {

  def main(args: Array[String]): Unit = {
    try {
      var num = 10/0;
    } catch {
      case ex:ArithmeticException => println("算术异常")
      case ex:Exception => println("Exception")
    } finally {
      println("finally ~~")
    }
  }
}
