package top.damoncai.scala.chapter07

object Demo_05_HighOperation {

  def main(args: Array[String]): Unit = {
    // 定义函数
    def fun(a:Int,b:Int):Int = {
      a+b
    }

    // 1.函数作为值传递

    def fun1 = fun _;

    def fun2:(Int,Int) => Int = fun;

    // 2.函数作为参数传递
    def fun3(func:(Int,Int) => Int,x:Int,y:Int):Int = {
      x + y
    }

    var res = fun3(fun,1,2);
    println(res)

    // 3.函数作为返回值
    def fun4():Int => Int = {
      def fun5(x:Int):Int = {
        x + 1;
      }
      fun5
    }

    println(fun4()(22))

  }
}
