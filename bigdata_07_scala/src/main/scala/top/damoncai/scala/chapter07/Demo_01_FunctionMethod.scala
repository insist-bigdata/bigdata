package top.damoncai.scala.chapter07

object Demo_01_FunctionMethod {

  def sayHello(name:String):Unit = {
    println(name + "say hello ~~");
  }

  def main(args: Array[String]): Unit = {

    def sayHello(name:String):Unit = {
      println(name + " say hello ~~");
    }

    sayHello("damoncai")

    Demo_01_FunctionMethod.sayHello("Rubby")
  }
}
