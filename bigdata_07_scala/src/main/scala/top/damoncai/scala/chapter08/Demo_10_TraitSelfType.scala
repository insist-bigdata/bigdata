package top.damoncai.scala.chapter08

object Demo_10_TraitSelfType {

  def main(args: Array[String]): Unit = {
    val user = new UserOper("damoncai","123")
    user.inster()
  }
}

trait UserDao {
  _:User =>
  def register(): Unit ={
    println("register == name: " + this.name)
  }
}

class User(var name:String, var pwd:String)

class UserOper(name:String, pwd:String) extends User(name, pwd) with UserDao {
  def inster(): Unit = {
    super.register()
  }
}


