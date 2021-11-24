package top.damoncai.scala.chapter08

object Demo_04_Polymorphism {
  def main(args: Array[String]): Unit = {

    val par:Parr = new Parr()
    println(par.name)
    par.printInfo()

    val son:Parr = new Sonn()
    println(son.name)
    son.printInfo()
  }
}

class Parr() {

  val name:String = "par"

  def printInfo(): Unit = {
    println("par")
  }
}


class Sonn extends Parr() {

  override val name:String = "son"

  override def printInfo(): Unit = {
    println("son")
  }
}
