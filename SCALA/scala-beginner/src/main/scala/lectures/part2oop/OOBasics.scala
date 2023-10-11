package lectures.part2oop

object OOBasics extends App{
  class Person(name: String, val age: Int) {
    // body
    val x = 2
    println(1 + 3)

    // method
    def greet(name: String) = println(s"${this.name} says: Hi, ${name}")

    // Overloading
    def greet(): Unit = println(s"Hi, I am $name")

    // multiple constructors
    def this(name:String) = this(name, 0)
    def this() = this("John Doe")
  }

  val person = new Person("John", 26)
  println(person.x)
  person.greet()
  person.greet("Simon")

  val author = new Writer("Charles", "Dickens", 1812)
  val imposter = new Writer("Charles", "Dickens", 1812)
  val novel = new Novel("Great Expectations", 1861, author)

  println(novel.authorAge)
  println(novel.isWrittenBy(imposter))


}

/*
  Novel and a Writer

  Writer: first name, surname, year
    - method fullname

  Novel: name, year of release, author
  - authorAge
  - isWrittenBy(author)
  - copy (new year of release) = new instance of Novel
 */
class Writer(firstName: String, lastName: String, val year: Int ){
  def fullName: String = firstName + " " + lastName
}

class Novel(name: String, year:Int, author: Writer){
  def authorAge = year-author.year
  def isWrittenBy(author:Writer) = author == this.author
  def copy(newYear: Int):Novel = new Novel(name, newYear, author)
}




