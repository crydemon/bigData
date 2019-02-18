package practise

//高阶函数倾向使用短的函数名
object MyModule {

  def abs(n: Int): Int = n match {
    case n if n < 0 => -n
    case _ => n
  }

  def abs1(n: Int): Int =
    if (n < 0) -n else n

  private def formatAbs(x: Int) = {
    val message = "The absoulute value of %d is %d"
    message.format(x, abs(x))
  }

  def factorial(n: Int): Int = {
    def go(n: Int, acc: Int): Int =
      if (n <= 0) acc else go(n - 1, n * acc)

    go(n, 1)
  }

  def fib(n: Int): Int = {
    def go(n: Int, a: Int, b: Int): Int =
      if (n <= 0) a else go(n - 1, b, a + b)

    go(n - 1, 0, 1)
  }

  private def formatFactorial(n: Int) = {
    val message = "The factorial of %d is %d."
    message.format(n, factorial(n))
  }

  def findFirst[A](as: Array[A], p: A => Boolean): Int = {
    def loop(n: Int): Int =
      if (n >= as.length) -1
      else if (p(as(n))) n
      else loop(n + 1)

    loop(0)
  }

  def isSorted[A](as: Array[A], ordered: (A, A) => Boolean): Boolean = {
    def loop(a: Int, b: Int): Boolean =
      if (b >= as.length) true
      else if (ordered(as(a), as(b))) loop(b, b + 1)
      else false

    loop(0, 1)
  }

  //partial application
  def partial1[A, B, C](a: A, f: (A, B) => C): B => C =
    (b: B) => f(a, b)

  def curry[A, B, C](f: (A, B) => C): A => (B => C) =
    (a: A) => ((b: B) => f(a, b))

  def uncurry[A, B, C](f: A => B => C): (A, B) => C =
    (a: A, b: B) => f(a)(b)

  def compose[A, B, C](f: B => C, g: A => B): A => C =
    (a: A) => f(g(a))


  def main(args: Array[String]) = {


    val curry1 = curry((a: Int, b: Int) => a + ", " + b)
    println(curry1(3)(4))
    println(uncurry(curry1)(3, 4))

    val f = (l: List[Int]) => l.filter(x => x % 3 == 0)
    val g = (l: List[Int]) => l.map(x => x * 3)
    println(compose(f, g)(List(1, 2, 3, 4, 5)))

    println(formatAbs(-534))
    println(factorial(11))
    (1 to 10).foreach(i => println(fib(i)))
    println(formatFactorial(5))
    println(findFirst("hat is king".toCharArray, (c: Char) => c == 'k'))
    println(isSorted("abcdef".toCharArray, (a: Char, b: Char) => a < b))
    println(isSorted("abdfdsgcdef".toCharArray, (a: Char, b: Char) => a < b))
  }

}

object test3 extends App {

  import MyModule._

  println(abs(-4))
  println(abs(4))
}
