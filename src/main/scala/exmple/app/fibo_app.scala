package exmple.app

object fibo_app {
  def main(args: Array[String]){
    def fact(x:Integer) :Integer = {
     if (x < 2) 1 else x*fact(x-1)
    }
    println(fact(7))
    
    def fizzbuzz(i:Int):Unit = {
      if (i<=100) {
        (i%3 , i%5) match {
          case(0,0) => println("Fizzbuzz")
          case(0,_) => println("Fizz")
          case(_,0) => println("buzz")
          case _ => println(i)
        }
        fizzbuzz(i+1)
      }
    }
    fizzbuzz(10)
    val a = Array(1, 2)
    val b = List(1, 4)
    println(a,b)
    
 //fibonaci series
  def fib1(n: Int): Int = n match {
   case 0 | 1 => n
   case _ => fib1(n-1) + fib1(n-2)
    }
    println(fib1(7))

  }
}