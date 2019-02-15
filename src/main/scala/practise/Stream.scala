package practise

//提升内存效率的流式计算
sealed trait Stream[+A] {

  import Stream._

  def headOption: Option[A] = this match {
    case Empty => None
    case Cons(h, _) => Some(h())
  }

  def headOption1: Option[A] =
    foldRight(None: Option[A])((a, _) => Some(a))

  def tailOption: Option[Stream[A]] = this match {
    case Empty => None
    case Cons(_, t) => Some(t())
  }

  def toList: List[A] = this match {
    case Empty => Nil
    case Cons(h, t) => h() :: t().toList
  }

  def toList2: List[A] = {
    @annotation.tailrec
    def go(s: Stream[A], acc: List[A]): List[A] = s match {
      case Cons(h, t) => go(t(), h() :: acc)
      case _ => acc
    }

    go(this, Nil).reverse
  }

  def take(n: Int): Stream[A] = this match {
    case Cons(h, t) if n > 1 => cons(h(), t().take(n - 1))
    case Cons(h, _) if n == 1 => cons(h(), empty)
    case _ => empty
  }

  def takeViaUnfold(n: Int): Stream[A] =
    unfold((this, n)) {
      case (Cons(h, _), 1) => Some(h(), (empty, 0))
      case (Cons(h, t), n) if n > 1 => Some(h(), (t(), n - 1))
      case _ => None
    }

  @annotation.tailrec
  final def drop(n: Int): Stream[A] = {
    if (n <= 0) this
    else this match {
      case Empty => Empty
      case Cons(_, t) => t().drop(n - 1)
    }
  }

  def takeWhile(f: A => Boolean): Stream[A] = this match {
    case Cons(h, t) if f(h()) => cons(h(), t().takeWhile(f))
    case _ => empty[A]
  }

  def takeWhile1(p: A => Boolean): Stream[A] =
    foldRight(empty[A])((a, b) => if (p(a)) cons(a, b) else empty[A])

  def takeWhileViaUnfold(f: A => Boolean): Stream[A] =
    unfold((this)) {
      case Cons(h, t) if f(h()) => Some(h(), t())
      case _ => None
    }


  def zipWith[B, C](s2: Stream[B])(f: (A, B) => C): Stream[C] =
    unfold((this, s2)) {
      case (Cons(h1, t1), Cons(h2, t2)) => Some((f(h1(), h2())), (t1(), t2()))
      case _ => None
    }

  def zipWithAll[B, C](s2: Stream[B])(f: (Option[A], Option[B]) => C): Stream[C] =
    unfold((this, s2)) {
      case (Empty, Empty) => None
      case (Cons(h, t), Empty) => Some(f(Some(h()), Option.empty[B]) -> (t(), empty[B]))
      case (Empty, Cons(h, t)) => Some(f(Option.empty[A], Some(h())) -> (empty[A] -> t()))
      case (Cons(h1, t1), Cons(h2, t2)) => Some(f(Some(h1()), Some(h2())) -> (t1() -> t2()))
    }


  def zipAll[B](s2: Stream[B]): Stream[(Option[A], Option[B])] =
    zipWithAll(s2)((_, _))

  def exists(p: A => Boolean): Boolean = this match {
    case Cons(h, t) => p(h()) || t().exists(p)
    case _ => false
  }


  //f第二个参数是传名参数
  def foldRight[B](z: => B)(f: (A, => B) => B): B =
    this match {
      case Cons(h, t) => f(h(), t().foldRight(z)(f))
      case _ => z
    }

  def exists1(p: A => Boolean): Boolean =
    foldRight(false)((a, b) => p(a) || b)

  def forAll(p: A => Boolean): Boolean = this match {
    case Cons(h, t) => p(h()) && t().forAll(p)
    case _ => true
  }

  def forAll1(p: A => Boolean): Boolean =
    foldRight(true)((a, b) => p(a) && b)


  def map[B](f: A => B): Stream[B] =
    foldRight(empty[B])((a, b) => cons(f(a), b))

  def filter(f: A => Boolean): Stream[A] =
    foldRight(empty[A])((a, b) => if (f(a)) cons(a, b) else b)

  //[B >: A] is a lower type bound. It means that B is constrained to be a supertype of A.
  def append[B >: A](s: => Stream[B]): Stream[B] =
    foldRight(s)((h, t) => cons(h, t))

  def find(p: A => Boolean): Option[A] =
    filter(p).headOption1

  def startWith[A](s: Stream[A]): Boolean =
    zipAll(s).takeWhile(!_._2.isEmpty) forAll {
      case (h, h2) => h == h2
    }

  /*
  The last element of `tails` is always the empty `Stream`,
  so we handle this as a special case, by appending it to the output.
  */
  def tails: Stream[Stream[A]] =
    unfold(this) {
      case Empty => None
      case s => Some(s, s drop 1)
    } append Stream(empty)

  def hasSubSequence[A](s: Stream[A]): Boolean =
    tails exists (_ startWith s)

  /*
  The function can't be implemented using `unfold`,
  since `unfold` generates elements of the `Stream` from left to right.
   It can be implemented using `foldRight` though.
  The implementation is just a `foldRight` that keeps the accumulated value and the stream of intermediate results,
  which we `cons` onto during each iteration. When writing folds,
  it's common to have more state in the fold than is needed to compute the result.
  Here, we simply extract the accumulated list once finished.
  */
  def scanRight[B](z: B)(f: (A, => B) => B): Stream[B] =
  // p0 is passed by-name and used in by-name args in f and cons.
  // So use lazy val to ensure only one evaluation...
    foldRight((z, Stream(z)))((a, p0) => {
      lazy val p1 = p0
      val b2 = f(a, p1._1)
      (b2, cons(b2, p1._2))
    })._2

}

case object Empty extends Stream[Nothing]

case class Cons[+A](h: () => A, t: () => Stream[A]) extends Stream[A]

object Stream {
  //普通方法
  def cons[A](hd: => A, tl: => Stream[A]): Stream[A] = {
    lazy val head = hd
    lazy val tail = tl
    Cons(() => head, () => tail)
  }

  def empty[A]: Stream[A] = Empty

  //想要标注一个重复参数，在参数的类型之后放一个星号
  //这个标注告诉编译器把 as 的每个元素当作参数
  def apply[A](as: A*): Stream[A] = {
    if (as.isEmpty) empty else cons(as.head, apply(as.tail: _*))
  }

  def constant[A](a: A): Stream[A] = {
    lazy val tail: Stream[A] = Cons(() => a, () => tail)
    tail
  }

  def from(n: Int): Stream[Int] = {
    Cons(() => n, () => from(n + 1))
  }

  val fib = {
    def go(f0: Int, f1: Int): Stream[Int] = {
      cons(f0, go(f1, f0 + f1))
    }

    go(0, 1)
  }

  def unfold[A, S](z: S)(f: S => Option[(A, S)]): Stream[A] =
    f(z) match {
      case Some((h, s)) => cons(h, unfold(s)(f))
      case None => empty
    }

  val fibsViaUnfold = unfold((0, 1)) {
    case (f0, f1) => Some((f0, (f1, f0 + f1)))
  }

  def from1(n: Int): Stream[Int] =
    unfold((n))((f0) => Some(f0, f0 + 1))

  def constant1[A](a: A): Stream[A] =
    unfold((a))((f0) => Some(f0, f0))

  val onesViaUnfold = constant1(1)
}

object test1 extends App {

  val x = Cons(() => {
    //println("e");
    1 + 2
  }, () => {
    //println("s");
    Stream({
      //println("qq");
      1 + 2
    }, 2, 4, 5, 5, 6, 7, 8)
  })
  val h1 = x.headOption
  val h2 = x.headOption
  val t1 = x.tailOption
  println(x.headOption1)
  println("------------------------")
  //强制求值
  println(t1.head.headOption)
  println(t1.head.headOption)
  println("------------------------")
  //5.1
  println(x.toList)
  println(x.take(4).toList)

  println(x.drop(4).toList)
  println("------------------------")
  println(x.takeWhile1((x: Int) => (x < 6)).toList)
  println(x.filter((x: Int) => (x < 4)).toList)
  println(x.find((x: Int) => (x >= 4)).toList)
}

object test2 extends App {
  val ones: Stream[Int] = Stream.cons(1, ones)
  println(ones.take(5).toList)
  println(Stream.constant(10).take(10).toList)
  println(Stream.from(10).take(10).toList)
  println(Stream.fib.take(10).toList)
  println(Stream.unfold((0, 1)) {
    case (f0, f1) => Some((f0, (f1, f0 + f1)))
  }.take(5).toList)
  println(Stream.from1(2).take(10).toList)
  println(Stream.constant1("wq").take(5).toList)
  println(Stream.onesViaUnfold.take(3).toList)
  val s1 = Stream(1, 32, 4, 5, 6)
  val s2 = Stream(34, 534, 56, 77567)
  val s3 = Stream(1, 32)
  println(s1.zipAll(s2).take(5).toList)
  println(s1 startWith s2)
  println(s1 startWith s3)
  s1.tails.take(3).toList.foreach(x => println(x.toList))
  println(s1.scanRight(0)(_ + _).toList)
}