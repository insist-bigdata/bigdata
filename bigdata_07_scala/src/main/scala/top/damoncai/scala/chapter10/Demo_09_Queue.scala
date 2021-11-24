package top.damoncai.scala.chapter10

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.parallel.immutable

object Demo_09_Queue {
  def main(args: Array[String]): Unit = {
    // 可变队列
    val queue = mutable.Queue[String]("a", "b", "c")
    println(queue.dequeue())
    println(queue)
    queue.enqueue("a")
    println(queue)

    println("===============")

    // 不可变队列
    var imQueue = Queue("a","b","c")

    println(imQueue.dequeue)
    println(imQueue)
  }
}
