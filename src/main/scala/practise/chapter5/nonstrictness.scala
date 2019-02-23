package practise.chapter5

//by name
object nonstrictness extends App {

  def if2[A](cond: Boolean, onTrue: () => A, onFalse: () => A): A =
    if (cond) onTrue() else onFalse()

  //自动包装成thunk
  def if3[A](cond: Boolean, onTrue: => A, onFalse: => A): A =
    if (cond) onTrue else onFalse

  if2(3 < 22, () => println(3), () => println(22))
  if2(true, () => println(3), () => sys.error("en"))
  if3(3 < 22, () => println(3), () => println(22))
  if3(true, println(3), sys.error("en"))

  def mayeTwice(b: Boolean, i: => Int) =
    if (b) i + i else 0

  mayeTwice(true, {
    println("hi"); 1 + 41
  })
  println("-------------------")
  def mayeTwice2(b: Boolean, i: => Int) ={
    lazy val j = i //延迟到第一次被引用，缓存结果
    if (b) j + j else 0
  }
  mayeTwice2(true, {
    println("hi"); 1 + 41
  })

}
