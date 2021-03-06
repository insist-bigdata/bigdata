package top.damoncai.scala.chapter07

object Demo_04_Lambada {

  def main(args: Array[String]): Unit = {

    //匿名函数赋值给变量
    val fun = (name:String) => {
      println(name)
    }

    //调用匿名函数
    fun("zhangsan");


    // 函数作为参数传输
    def f(func:String => Unit):Unit = {
      func("aiguigu")
    }

    f(fun);
    f((name:String) => {println(name)})

    //匿名函数简化原则
    // 1.参数的类型可以省略，会根据形参进行自动的推导
    f((name) => {println(name)})

    // 2.类型省略之后，发现只有一个参数，则圆括号可以省略；其他情况：没有参数和参数超过 1 的永远不能省略圆括号。
    f(name => { println(name)})

    // 3.匿名函数如果只有一行，则大括号也可以省略
    f(name => println(name))

    // 4.如果参数只出现一次，则参数省略且后面参数可以用_代替
    f(println(_))
    // 5.如果可以推断出，当前传入的println是个函数体，而不是调用语句，可以直接省略下划线
    f(println)
  }
}
