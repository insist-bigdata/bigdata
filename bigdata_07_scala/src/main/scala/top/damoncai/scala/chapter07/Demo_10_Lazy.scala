package top.damoncai.scala.chapter07

object Demo_10_Lazy {

  def main(args: Array[String]): Unit = {
    lazy val res = fun();

    println("1.hello ")
    println("2.res " + res)

    def fun():Unit = {
      println("fun被调用")
    }
  }
}
