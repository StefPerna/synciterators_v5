
package synchrony
package iterators

//
// Wong Limsoon
// 16/1/2020
//


object MiscCollections {


  import scala.collection.mutable.Queue


  trait Shadowing[A] 
  { //
    // Shadowing is a trait that provides a "primary" iterator X
    // with a "shadow iterator" Y which iterates over the same
    // elems as iterator X. However, every elem read by iterator Y
    // is buffered for this main iterator X to read again.  On the
    // other hand, every elem read by X itself is gone forever even
    // if Y has not seen it yet.

    protected val elems: Iterator[A]
    //
    // elems is the only feature to be overriden by inheriting classes.
    // elems is the original unshadowed iterator.

    final protected val buffer = new Queue[A]()
  
    final val primary = new Iterator[A] {
      override def hasNext = (!buffer.isEmpty) || elems.hasNext
      override def next() =
        if (buffer.isEmpty) elems.next() else buffer.dequeue()
    }
      
    final def shadow() = new Iterator[A]
    {
      val buff = buffer.clone

      override def hasNext = (!buff.isEmpty) || elems.hasNext

      override def next() =
        if (!buff.isEmpty) buff.dequeue() else {
          buffer.enqueue(elems.next())
          buffer.last
      }
    }
  }    
 

  object ShadowedIterator
  {
    type ShadowedIterator[A] = Iterator[A] with Shadowing[A]

    def apply[A](it: Iterator[A]): ShadowedIterator[A] =
      new Iterator[A] with Shadowing[A] {
          override val elems = it
          override def hasNext = primary.hasNext
          override def next() = primary.next()
      }
  }




  trait Sliding2[A]
  { //
    // Convert an iterator to a sliding iterator with window size 2.

    protected val itS: Iterator[A]
    protected var bufS: Option[A]
    //
    // buf should be initialized to the 1st elem of the iterator
    // slide over. itS is the iterator to slide over.


    final def nextStep(): (A,A) = { 
      val x = bufS.get
      val y = itS.next()
      bufS = Some(y)
      return (x, y)
    }

    final def hasNextStep: Boolean = itS.hasNext
  }

  
  object SlidingIterator2
  {
    type SlidingIterator2[A] = Iterator[(A,A)] with Sliding2[A]

    def apply[A](it:Iterator[A]): SlidingIterator2[A] = 
      new Iterator[(A,A)] with Sliding2[A] {
        override val itS = it
        override var bufS = if (itS.hasNext) Some(itS.next()) else None
        override def hasNext = hasNextStep
        override def next() = nextStep()
    }
  }



  trait SlidingN[A]
  { //
    // Convert an iterator to a sliding iterator with window size N.

    protected val itS: Iterator[A]
    protected var bufS: Option[Vector[A]]
    //
    // bufS should be initialized to the first n-1 elems of 
    // the iterator to slide over. itS is the iterator to
    // slide over.


    final def nextStep(): Vector[A] = { 
      val x = bufS.get
      val y = itS.next()
      val z = x :+ y
      bufS = Some(z.drop(1))
      return z
    }

    final def hasNextStep: Boolean = itS.hasNext
  }


  object SlidingIteratorN
  {
    type SlidingIteratorN[A] = Iterator[Vector[A]] with SlidingN[A]

    def apply[A](n:Int)(it:Iterator[A]): SlidingIteratorN[A] = 
      new Iterator[Vector[A]] with SlidingN[A] {
        override val itS = it
        var buf = Vector[A]()
        var i = 1
        while ((i<n) && itS.hasNext) {i = i + 1; buf = buf :+ (itS.next())}
        override var bufS = if (i==n) Some(buf) else None
        override def hasNext = hasNextStep
        override def next() = nextStep()
    }
  }
  
}


/*
 * Example
 *

import synchrony.iterators.MisCollections._

val a= Iterator(1,2,3,4,5,6,7,8,9)
val b= SlidingIteratorN(3)(a)
for(x <- b) println(x)

 *
 * Expected output
 *

scala> for(x <-b) println(x)
Vector(1, 2, 3)
Vector(2, 3, 4)
Vector(3, 4, 5)
Vector(4, 5, 6)
Vector(5, 6, 7)
Vector(6, 7, 8)
Vector(7, 8, 9)

 *
 */


