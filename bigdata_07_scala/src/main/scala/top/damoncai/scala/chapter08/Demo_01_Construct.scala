package top.damoncai.scala.chapter08

object Demo_01_Construct {
  def main(args: Array[String]): Unit = {
    var person = new Person("臧三",12)
  }
}

// 如果主构造器无参数，小括号可省略
class Person() {

  var name:String = _;

  var age:Int = _;

  def this(name:String) {
    this()
    this.name = name
    println("辅助构造器")
  }

  def this(name:String,age:Int) {
    this(name);
    this.age = age;
    println("辅助构造器")
  }

}
