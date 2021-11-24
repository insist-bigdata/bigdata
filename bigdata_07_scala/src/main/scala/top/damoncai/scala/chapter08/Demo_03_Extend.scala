package top.damoncai.scala.chapter08

object Demo_03_Extend {

  def main(args: Array[String]): Unit = {
//    var par = new Par("张三",3);
      var son1 = new Son1("张三",3)
      println(son1.name) // null

    var son2 = new Son2("李四",4)
    println(son2.name) // 李四
  }
}

class Par() {

  var name:String = _
  var age:Int = _

  println("父 - 主 - 构造器")

  def say(): Unit = {
    println("par")
  }

  def this(name:String,age:Int) {
    this()
    this.name = name
    this.age = age
    println("父 - 辅 - 构造器")
  }
}

class Son1(name:String,age:Int) extends Par() {
  var no:String = _
  def this(name:String,age:Int,no:String) {
    this(name,age)
    this.no = no
  }
}

class Son2(name:String,age:Int) extends Par(name,age) {
  var no:String = _
  def this(name:String,age:Int,no:String) {
    this(name,age)
    this.no = no
  }
}
