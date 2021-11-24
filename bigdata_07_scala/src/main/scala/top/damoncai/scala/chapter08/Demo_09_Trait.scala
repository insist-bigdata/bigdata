package top.damoncai.scala.chapter08

trait Demo_09_Trait {

  // 声明属性
  var name:String = _
  // 声明方法
  def eat():Unit={
  }
  // 抽象属性
  var age:Int

  // 抽象方法
  def say():Unit

}

trait PersonTrait {
  //（1）特质可以同时拥有抽象方法和具体方法
  // 声明属性
  var name: String = _
  // 抽象属性
  var age: Int
  // 声明方法
  def eat(): Unit = {
    println("eat")
  }
  // 抽象方法
  def say(): Unit
}

trait SexTrait {
  val sex: String
}

//（2）一个类可以实现/继承多个特质
//（3）所有的 Java 接口都可以当做 Scala 特质使用
class Teacher extends PersonTrait with SexTrait with java.io.Serializable {
  override def say(): Unit = {
    println("say")
  }
  override var age: Int = _
  override val sex: String = ""
}
object TestTrait {
  def main(args: Array[String]): Unit = {
    val teacher = new Teacher
    teacher.say()
    teacher.eat()
    //（4）动态混入：可灵活的扩展类的功能
    val t2 = new Teacher with SexTrait {
      override val sex: String = "男"
    }
    //调用混入 trait 的属性
    println(t2.sex)
  }
}



