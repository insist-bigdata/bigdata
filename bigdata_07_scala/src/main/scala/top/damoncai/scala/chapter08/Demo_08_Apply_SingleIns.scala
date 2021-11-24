package top.damoncai.scala.chapter08

// 伴生对象
object Demo_08_Apply_SingleIns {
  def main(args: Array[String]): Unit = {
    val dog1 = Dog.getInstance();
    val dog2 = Dog.getInstance();
    val dog3 = Dog.apply();
    //通过伴生对象的 apply 方法，实现不使用 new 关键字创建对象。
    val dog4 = Dog();
    println(dog1 == dog2) // true
    println(dog1 == dog3) // true
    println(dog1 == dog4) // true
  }
}

class Dog private () {
}

object Dog {
  var dog:Dog = null;
  def getInstance():Dog = {
    if(dog == null) dog = new Dog();
    dog
  }

  def apply():Dog = {
    println("apply ~~")
    if(dog == null) dog = new Dog();
    dog
  }
}


