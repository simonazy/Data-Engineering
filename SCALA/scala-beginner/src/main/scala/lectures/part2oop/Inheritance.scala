package lectures.part2oop

object Inheritance extends App{
      protected class Animal{
        val creatureType = "wild"
        def eat = println("nomnom")
      }

      class Cat extends Animal{
        def crunch = {
          eat
          println("crunch crunch")
        }
      }

      val cat = new Cat()
      cat.crunch

  // with constructor
    class Person(name: String, age: Int){
      def this(name: String) = this(name, 0)
    }
    class Adult(name:String, age: Int, idCard: String) extends Person(name)

  //Override
  class Dog(override val creatureType: String) extends Animal{
    // override val creatureType= "domestic" // can override in the body or directly in the constructor arguments
      override def eat = {
        super.eat
        println(("woof woof"))
      }
    }

  val dog = new Dog("K9")
  dog.eat
  println(dog.creatureType)

  //Override vs overloading

  // Prevents override
  // 1 - use final on member
  // 2 - use final on the entire class
  // 3 - seal the class = extends classes in this File, prevent extensions in other files.

}
