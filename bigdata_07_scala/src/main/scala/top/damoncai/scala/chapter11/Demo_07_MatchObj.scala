package top.damoncai.scala.chapter11

object Demo_07_MatchObj {
  def main(args: Array[String]): Unit = {
    // 匹配对象
    var bobe = new Student("bobe",13);

    bobe match {
      case Student("bobe",12) => println("name:bobe,age:12")
      case _ => println("else")
    }
  }
}

class Student(var name:String,var age:Int)

object Student{
  def apply(name: String, age: Int): Student = new Student(name, age)
  def unapply(student: Student):Option[(String,Int)] = Some(student.name,student.age)
}
