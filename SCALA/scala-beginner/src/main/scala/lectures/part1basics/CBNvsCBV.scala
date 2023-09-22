package lectures.part1basics

object CBNvsCBV extends App{
  def callByValue(x: Long):Unit = {
    println("by value: " + x)
    println("by value: " + x)
  }

  def callByName(x: =>Long)={
    println("by name: " + x)
    println("by name: " + x)
  }

  callByValue(System.nanoTime()) // Value is computed before call, same value used everywhere
  callByName(System.nanoTime()) // expression is passed literally, expression is evaluated at every use within

  def infinite():Int = 1 + infinite()
  def printFirst(x: Int, y: => Int) = print(x)

  //printFirst(infinite(), 34) // stack overflow
  printFirst(3, infinite())
}
