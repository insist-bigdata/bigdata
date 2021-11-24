package top.damoncai.scala.chapter07

object Demo_03_Simple {

  def main(args: Array[String]): Unit = {
    //    （1）return 可以省略，Scala 会使用函数体的最后一行代码作为返回值
    def fun1(name: String): String = {
      "abc"
    }

    //    （2）如果函数体只有一行代码，可以省略花括号
    def fun2(name: String): String = "abc"

    //    （3）返回值类型如果能够推断出来，那么可以省略（:和返回值类型一起省略）
    def fun3(name: String) = "abc"

    //    （4）如果有 return，则不能省略返回值类型，必须指定
    def fun4(name: String): String = return "abc"

    //    （5）如果函数明确声明 unit，那么即使函数体中使用 return 关键字也不起作用
    def fun5(name: String): Unit = return "abc"

    println(fun5("damocnai")) // ()
    //    （6）Scala 如果期望是无返回值类型，可以省略等号
    def fun6(name: String){
      println(name)
    }
    fun6("xiix")
    //    （7）如果函数无参，但是声明了参数列表，那么调用时，小括号，可加可不加
    def fun7(): Unit = return "abc"
    fun7()
    fun7
    //    （8）如果函数没有参数列表，那么小括号可以省略，调用时小括号必须省略
    def fun8: Unit = return "abc"
    fun8
    //    （9）如果不关心名称，只关心逻辑处理，那么函数名（def）可以省略
    (name:String) => {
      println(name)
    }
  }
}
