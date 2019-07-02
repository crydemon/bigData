package concurrent

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import scala.util.Try
object KFuture {
  type CoffeeBeans = String
  type GroundCoffee = String

  case class Water(temperature: Int)

  type Milk = String
  type FrothedMilk = String
  type Espresso = String
  type Cappuccino = String


  // some exceptions for things that might go wrong in the individual steps
  // (we'll need some of them later, use the others when experimenting with the code):
  case class GrindingException(msg: String) extends Exception(msg)

  case class FrothingException(msg: String) extends Exception(msg)

  case class WaterBoilingException(msg: String) extends Exception(msg)

  case class BrewingException(msg: String) extends Exception(msg)

  def grind(beans: CoffeeBeans): Future[GroundCoffee] = Future {
    println("start grinding...")
    Thread.sleep(1000)
    if (beans.equals("baked beans")) throw GrindingException("are you joking?")
    println("finished grinding...")
    s"ground coffee of $beans"
  }

  def heatWater(water: Water): Future[Water] = Future {
    println("heating the water now")
    Thread.sleep(Random.nextInt(2000))
    println("hot, it's hot!")
    water.copy(temperature = 85)
  }

  def frothMilk(milk: Milk): Future[FrothedMilk] = Future {
    println("milk frothing system engaged!")
    Thread.sleep(Random.nextInt(2000))
    println("shutting down milk frothing system")
    s"frothed $milk"
  }

  def brew(coffee: GroundCoffee, heatedWater: Water): Future[Espresso] = Future {
    println("happy brewing :)")
    Thread.sleep(Random.nextInt(2000))
    println("it's brewed!")
    "espresso"
  }

  def main(args: Array[String]): Unit = {
    import scala.util.{Success, Failure}
    println("start")
//    val grindFuture = grind("baked beans").onComplete {
//      case Success(ground) => println(s"got my $ground")
//      case Failure(ex) => println("This grinder needs a replacement, seriously!")
//    }
//    val tempreatureOkay: Future[Boolean] = heatWater(Water(25)) map { water =>
//      println("we're in the future!")
//      (80 to 85) contains (water.temperature)
//    }
    Await result(grind("baked beans"), 3 minutes)























  }
}
