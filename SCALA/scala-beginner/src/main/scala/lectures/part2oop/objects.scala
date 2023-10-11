package lectures.part2oop

object objects extends App{
  // SCALA does not have class-level functionality ("static")
  object Person {
    // class-level functionality
    val N_EYES = 2
    def canFLY:Boolean = false
  }

  class Person(val name:String) {
    //instance-level functionality
  }
  //COMPANIONS

  println(Person.N_EYES)
  println(Person.canFLY)
  // SCALA object is a singleton instance.
  val mary = Person
  val tom = Person
  println(mary == tom)
}
