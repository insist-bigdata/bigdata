package top.damoncai.scala.chapter07

object Demo_09_ControlAbstract {

  def main(args: Array[String]): Unit = {

    // 控制抽象 - 传值参数
    def fun(name:String) : Int = {
      println("fun被调用")
      println(name)
      12
    }
    fun("damoncai")

    // 控制抽象 - 传名参数 - 传递的是代码块
    println("========================================")
    def func(arg: => Int): Unit = {
      println("arg: " + arg)
      println("arg: " + arg)
    }
    func({12})
    func(fun("damoncai"))
  }
}
