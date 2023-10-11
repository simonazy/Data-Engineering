package lectures.part2oop

object methodNotations extends App{
  class Person(val name: String, favoriteMovie: String){
    def likes(movie:String) = this.favoriteMovie == movie
    def +(person: Person)= s"${this.name} is hanging out with ${person.name}"
    def isAlive: Boolean = true
    def apply():String = s"Hi, my name is $name and I like $favoriteMovie"
  }

  val mary = new Person("Mary", "Inception")
  println(mary.likes("Inception"))
  println(mary likes "Inception")

  //"operators" in scala
  val tom = new Person("Tom", "Fight club")
  println(mary + tom)
  println(mary.+(tom))

  // prefix notation
  val x = -1
  val y = 1.unary_-  //unary_prefix only works with - + ~ !

  println(mary.apply())
  println(mary())  //equivalent
}
