package top.damoncai.scala.chapter01

/**
 * object：关键字，声明一个单例对象(伴生对象)
 */
object Demo_01_HelloWorld {

  /**
   * mian 方法：外部可执行的方法
   * def 方法名称(参数名：参数类型)：返回数据类型 = {方法体}
   * @param args
   */
  def main(args: Array[String]): Unit = {
    println("Hello World");
    println(Math.max(10,12)); // 调用java代码
  }
}
