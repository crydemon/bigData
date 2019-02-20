package practise.chapter3

//函数式数据结构，只能被纯函数操作

sealed trait List[+A]

case object Nil extends List[Nothing]

case class Cons[+A](head: A, tail: List[A]) extends List[A]

object List {
  def sum(ints: List[Int]): Int = ints match {
    case Nil => 0
    case Cons(h, t) => h + sum(t)
  }

  def product(ds: List[Double]): Double = ds match {
    case Nil => 1.0
    case Cons(0.0, _) => 0.0
    case Cons(h, t) => h * product(t)
  }

  def tail[A](l: List[A]): List[A] =
    l match {
      case Nil => Nil
      case Cons(_, t) => t
    }

  def drop[A](l: List[A], n: Int): List[A] =
    if (n <= 0) l
    else drop(tail(l), n - 1)

  def dropWhile[A](l: List[A], f: A => Boolean): List[A] =
    l match {
      case Cons(head, tail) if (f(head)) => dropWhile(tail, f)
      case _ => l
    }

  //通过将函数参数分钟排序成多个参数列表，来最大化的利用类型推导
  def dropWhile1[A](as: List[A])(f: A => Boolean): List[A] =
    as match {
      case Cons(head, tail) if f(head) => dropWhile1(tail)(f)
      case _ => as
    }

  def append[A](a1: List[A], a2: List[A]): List[A] =
    a1 match {
      case Nil => a2
      case Cons(h, t) => Cons(h, append(t, a2))
    }

  def init[A](l: List[A]): List[A] =
    l match {
      case Nil => Nil
      case Cons(_, Nil) => Nil
      case Cons(head, tail) => Cons(head, init(tail))
    }

  def foldRight[A, B](as: List[A], z: B)(f: (A, B) => B): B =
    as match {
      case Nil => z
      case Cons(x, xs) => f(x, foldRight(xs, z)(f))
    }

  def sum2(ns: List[Int]) =
    foldRight(ns, 0)(_ + _)

  def product2(ns: List[Double]) =
    foldRight(ns, 1.0)(_ * _)


  /*
  No, this is not possible! The reason is because _before_ we ever call our function, `f`, we evaluate its argument,
  which in the case of `foldRight` means traversing the list all the way to the end. We need _non-strict_ evaluation
  to support early termination---we discuss this in chapter 5.
  */

  /*
  We get back the original list! Why is that? As we mentioned earlier, one way of thinking about what `foldRight` "does"
  is it replaces the `Nil` constructor of the list with the `z` argument, and it replaces the `Cons` constructor with
  the given function, `f`. If we just supply `Nil` for `z` and `Cons` for `f`, then we get back the input list.
  foldRight(Cons(1, Cons(2, Cons(3, Nil))), Nil:List[Int])(Cons(_,_))
  Cons(1, foldRight(Cons(2, Cons(3, Nil)), Nil:List[Int])(Cons(_,_)))
  Cons(1, Cons(2, foldRight(Cons(3, Nil), Nil:List[Int])(Cons(_,_))))
  Cons(1, Cons(2, Cons(3, foldRight(Nil, Nil:List[Int])(Cons(_,_)))))
  Cons(1, Cons(2, Cons(3, Nil)))
  */

  def length[A](as: List[A]): Int =
    as match {
      case Nil => 0
      case Cons(_, tail) => 1 + length(tail)
    }


  def sum3(ns: List[Int]): Int =
    foldLeft(ns, 0)(_ + _)

  def product3(ns: List[Double]): Double =
    foldLeft(ns, 1.0)(_ * _)

  def length1[A](l: List[A]): Int = foldLeft(l, 0)((acc, _) => acc + 1)

  def length2[A](as: List[A]): Int =
    foldRight(as, 0)((_, acc) => acc + 1)


  def foldLeft[A, B](l: List[A], z: B)(f: (B, A) => B): B = l match {
    case Nil => z
    case Cons(h, t) => foldLeft(t, f(z, h))(f)
  }

  def reverse[A](l: List[A]): List[A] =
    foldLeft(l, List[A]())((acc, h) => Cons(h, acc))

  def append1[A](l1: List[A], l2: List[A]): List[A] =
    foldLeft(reverse(l1), l2)((b, a) => Cons(a, b))


  /*
  `append` simply replaces the `Nil` constructor of the first list with the second list, which is exactly the operation
  performed by `foldRight`.
  */
  def appendViaFoldRight[A](l: List[A], r: List[A]): List[A] =
    foldRight(l, r)(Cons(_, _))

  /*
    The implementation of `foldRight` in terms of `reverse` and `foldLeft` is a common trick for avoiding stack overflows
    when implementing a strict `foldRight` function as we've done in this chapter. (We'll revisit this in a later chapter,
    when we discuss laziness).
    The other implementations build up a chain of functions which, when called, results in the operations being performed
    with the correct associativity. We are calling `foldRight` with the `B` type being instantiated to `B => B`, then
    calling the built up function with the `z` argument. Try expanding the definitions by substituting equals for equals
    using a simple example, like `foldLeft(List(1,2,3), 0)(_ + _)` if this isn't clear. Note these implementations are
    more of theoretical interest - they aren't stack-safe and won't work for large lists.
  */
  def foldRightViaFoldLeft[A, B](l: List[A], z: B)(f: (A, B) => B): B =
    foldLeft(reverse(l), z)((b, a) => f(a, b))

  def foldLeftViaFoldRight[A, B](l: List[A], z: B)(f: (B, A) => B): B =
    foldRight(l, (b: B) => b)((a, g) => b => g(f(b, a)))(z)

  def concat[A](l: List[List[A]]): List[A] =
    foldRightViaFoldLeft(l, Nil: List[A])(append)

  def addOne(l: List[Int]): List[Int] =
    foldRight(l, Nil: List[Int])((h, t) => Cons(h + 1, t))

  def doubleToString(l: List[Double]): List[String] =
    foldRight(l, Nil: List[String])((h, t) => Cons(h.toString, t))

  def map[A, B](l: List[A])(f: A => B): List[B] =
    foldRight(l, Nil: List[B])((h, t) => Cons(f(h), t))

  def map1[A, B](as: List[A])(f: A => B): List[B] =
    as match {
      case Cons(h, t) => Cons(f(h), map(t)(f))
      case Nil => Nil
    }

  def map2[A, B](l: List[A])(f: A => B): List[B] =
    foldRightViaFoldLeft(l, Nil: List[B])((h, t) => Cons(f(h), t))

  def map3[A, B](l: List[A])(f: A => B): List[B] = {
    //带备忘录的至顶向下
    val buf = new collection.mutable.ListBuffer[B]

    def go(l: List[A]): Unit = l match {
      case Nil => ()
      case Cons(h, t) => buf += f(h); go(t)
    }

    go(l)
    List(buf.toList: _*)
  }

  def filter[A](as: List[A])(f: A => Boolean): List[A] =
    foldRight(as, Nil: List[A])((h, t) => if (f(h)) Cons(h, t) else t)

  def flatMap[A, B](as: List[A])(f: A => List[B]): List[B] =
    concat(map(as)(f))

  def filterViaFlatMap[A](l: List[A])(f: A => Boolean): List[A] =
    flatMap(l)(a => if (f(a)) List(a) else Nil)

  def addPairwise(a: List[Int], b: List[Int]): List[Int] = (a, b) match {
    case (Nil, _) => Nil
    case (_, Nil) => Nil
    case (Cons(h1, t1), Cons(h2, t2)) => Cons(h1 + h2, addPairwise(t1, t2))
  }

  /*
  This function is usually called `zipWith`.
  The discussion about stack usage from the explanation of `map` also
  applies here. By putting the `f` in the second argument list,
  Scala can infer its type from the previous argument list.
  */
  def zipWith[A, B, C](a: List[A], b: List[B])(f: (A, B) => C): List[C] = (a, b) match {
    case (Nil, _) => Nil
    case (_, Nil) => Nil
    case (Cons(h1, t1), Cons(h2, t2)) => Cons(f(h1, h2), zipWith(t1, t2)(f))
  }

  @annotation.tailrec
  def startsWith[A](l: List[A], prefix: List[A]): Boolean = (l, prefix) match {
    case (_, Nil) => true
    case (Cons(h, t), Cons(h2, t2)) if h == h2 => startsWith(t, t2)
    case _ => false
  }

  @annotation.tailrec
  def hasSubsequence[A](sup: List[A], sub: List[A]): Boolean = sup match {
    case Nil => sub == Nil
    case _ if startsWith(sup, sub) => true
    case Cons(h, t) => hasSubsequence(t, sub)
  }


  //在伴生对象中定义一个可变参数的apply方法以便构造这个数据类型的实例是一种惯例
  def apply[A](as: A*): List[A] =
    if (as.isEmpty) Nil
    else Cons(as.head, apply(as.tail: _*))

}

object ListTest extends App {

  import List._

  val l = List(1, 2, 1, 34, 3, 3, 4, 5, 6)
  val l1 = List(33, 34, 35)
  val l2 = List(3423)
  val l3 = List(1.3, 43, 43.2, 34)
  println(append1(l1, l))
  println(length1(l))
  println(length2(l))
  println(product2(l3))
  println(sum2(l))
  println(dropWhile1(l)(i => i < 3))
  println(init(l))
  println(init(Nil))
  println(init(l2))
  println(append(l1, l))
  println(dropWhile(l, (i: Int) => i < 3))
  println(drop(l, 3))
  println(tail(l))
  println(tail(Nil))
}
