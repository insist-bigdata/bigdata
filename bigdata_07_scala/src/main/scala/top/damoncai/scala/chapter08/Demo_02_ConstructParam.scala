package top.damoncai.scala.chapter08

object Demo_02_ConstructParam {

  def main(args: Array[String]): Unit = {
    var personA = new PersonA("张三",12)
    println(personA.name + " " + personA.age)

    var personB = new PersonB("王五",15)
    // personB.name // err

    var personC = new PersonC("赵六")
    var personCC = new PersonC("钱七",17)

    println(personC.name)
    println(personCC.name)
  }
}

class PersonA(var name:String,var age:Int) {
  println("hello")
}

class PersonB(name:String,age:Int) {
  println("hello")
}

class PersonC(var name:String) {
  var age:Int = _
  def this(name:String,age:Int) {
    this(name)
    this.age = age;
  }
}
