
package synchrony
package programming


object Arith {

  abstract class Arith[A] {
    def addA(x:A, y:A):A
    def subA(x:A, y:A):A 
    def mulA(x:A, y:A):A 
    def divA(x:A, y:A):A 
  }

 implicit val intArith = new Arith[Int] {
   def addA(x:Int, y:Int) = x + y
   def subA(x:Int, y:Int) = x - y
   def mulA(x:Int, y:Int) = x * y
   def divA(x:Int, y:Int) = x / y
 }

 implicit val doubleArith = new Arith[Double] {
   def addA(x:Double, y:Double) = x + y
   def subA(x:Double, y:Double) = x - y
   def mulA(x:Double, y:Double) = x * y
   def divA(x:Double, y:Double) = x / y
 }
}

 

