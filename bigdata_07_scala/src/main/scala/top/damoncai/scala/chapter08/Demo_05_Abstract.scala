package top.damoncai.scala.chapter08

object Demo_05_Abstract {
  def main(args: Array[String]): Unit = {
    val par:AbsPar = new APar

    println(par.name)
    println(par.age)

    par.say()

    par.say2()
  }
}

abstract class AbsPar {

  val name:String;

  var age = 12;

  def say():Unit;

  def say2(): Unit = {
    println("hello")
  }
}

class APar extends AbsPar {
  override val name: String = "张三"

  age = 3;

  override def say(): Unit = {
    println("say")
  }
}
