package lectures.exercies

import scala.collection.immutable.Stream.Empty

abstract class MyList {
  /*
      head = first element of  the  list
      tail = remainder of the list
      isEmpty = is this list empty
      add(int) => new list with this element added
      toString => a string representation of the list
    */
  def head: Int
  def tail: MyList
  def isEmpty: Boolean
  def add(element: Int) : MyList
  def printElements: String
  // polymorphic call
  override def toString: String = "[" + printElements + "]"
}

class Emptys extends MyList{
  def head: Int = throw new NoSuchElementException
  def tail: MyList = throw new NoSuchElementException
  def isEmpty: Boolean = false
  def add(element: Int): MyList = new Cons(element, Emptys.this)
  def printElements : String = ""
}

class Cons(h: Int, t: MyList) extends MyList{
  def head: Int = h
  def tail: MyList = t
  def isEmpty: Boolean = false
  def add(element: Int): MyList = new Cons(element, this)
  def printElements: String =
    if(t.isEmpty) "" + h
    else h + " " + t.printElements
}

object ListTest extends App{
  val list = new Cons(1, new Cons(2, new Emptys))
  println(list.add(3).head)
  println(list.tail.head)
  println(list.isEmpty)
  println(list.toString)
}



