package lectures.part1basics

import scala.annotation.tailrec

object recursion extends App{
    def factorial(n:Int):Int =
      if (n<=1) 1
      else {
        println("Computing factorial of "+n+" - first need to compute "+(n-1))
        val result = factorial(n-1)*n
        println("Computed factorial of " + n)
        result
      }

    println(factorial(10))
    //  println(factorial(5000)) // stack overflow!

  def anotherFactorial(n:Int): BigInt = {
    def factHelper(x: Int, accumulator: BigInt): BigInt = {
      if (x <= 1) accumulator
      else factHelper(x - 1, x * accumulator)}

    factHelper(n, 1)
    }

  println(anotherFactorial(20000))
}


