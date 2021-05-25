
package synchrony.iterators  

/** AggrCollections provide a trait, Aggregates, to endow iterators
 *  and other common collection types with groupby and common 
 *  aggregate functions.
 *
 * Wong Limsoon
 * 25 April 2020
 */



object AggrCollections {

  import scala.collection.mutable.ArrayBuffer
  import scala.collection.mutable.Map
  import scala.language.existentials
  import synchrony.programming.Sri
  import synchrony.programming.Sri._


  type AggrIterator[C] = Iterator[C] with Aggregates[C]


  /** Trait Aggregates endows inheriting classes with some
   *  commonly used aggregate functions and groupby operators.
   *
   *  The function [[itC]] must be defined by inheriting classes.
   *  [[itC]] is the iterator that the aggregate functions iterate on.
   */

  trait Aggregates[C] {


  /** itC defines the iterator to be used by aggregate functions.
   *  itC is the only feature to be overriden in inheriting classes.
   */

    def itC: Iterator[C]



  /** groups(f) is the set of grouping keys. 
   *  This is mostly for info only.
   *
   *  @param f defines the grouping key.
   *  @return the grouping keys in [[itC]].
   */

    def groups[D](f: C => D): Vector[D] = (for (c <- itC) yield f(c)).toVector



  /** Group elements in [[itC]] and apply an aggregate function 
   *  to each group.
   *
   *  groupby(f)(aggr) groups the elements on this iterator by f,
   *  and then apply the aggregate function aggr on each group.
   *  Actually, aggr is apply on the fly as each elem is inserted 
   *  into its corresponding group.
   *
   *  @param f defines the grouping key.
   *  @param aggr is the aggregate function to be used on each group.
   *  @return a map from grouping key to aggregate function result.
   */

    def groupby[K, F](f: C => K)(aggr: Sri[C, F]): Map[K, F] =
      AggrIterator.groupby[C, K, F](itC)(f)(aggr)
    


  /** Group elements in [[itC]] and apply an aggregate function 
   *  to each group.
   *
   *  partitionby(f)(aggr) splits this iterator at each position i
   *  where f(xi) and f(xi+1) differ. Then applies the aggregate
   *  function aggr to each chunk. Actually it applies aggr on the
   *  fly as each xi is inserted into its own chunk. It has basically 
   *  same output as groupby(f)(aggr) when the iterator is sorted
   *  on f, but is much more efficient than groupby.  
   *
   *  @param f defines the grouping key.
   *  @param aggr is the aggregate function to be used on each group.
   *  @return an iterator of grouping key-aggregate function result
   *          pairs.
   */

    def partitionby[K, F](f: C => K)(aggr: Sri[C, F]): Iterator[(K, F)] =
      AggrIterator.partitionby[C, K, F](itC)(f)(aggr)
    


  /** Apply an aggregate function to elements in this iterator.
   *
   *  @param aggr is the aggregate function to be applied.
   *  @return result of applying [[aggr]], or Vector() if
   *          this iterator is empty.
   */
    
    def aggregateBy[F](aggr: Sri[C, F]): Vector[F] =
      AggrIterator.aggregateBy[C,F](itC)(aggr)



  /** Apply an aggregate function to elements in this iterator.
   *
   *  @param aggr is the aggregate function to be applied.
   *  @return result of applying [[aggr]].
   */

    def flatAggregateBy[F](aggr: Sri[C, F]):F =
      AggrIterator.flatAggregateBy[C, F](itC)(aggr)

  } // End trait Aggregates



  /** OpG provides some commonly used aggregate functions.
   *
   *  Here are some commonly used "aggregate functions"
   *  specified in terms of Sri:
   *  - minimize(f), elements which minimize f;
   *  - maximize(f), elements which maximize f;
   *  - map(f), applies f to each element;
   *  - ext(f), flatmaps f to each element;
   *  - keep, is basically map(x=>x);
   *  - average(f), computes the average of f of each element;
   *  - sum(f), computes the sum of f of each element;
   *  - count,  counts the number of elements, basically it is sum(x=>1);
   *  - concat(f), concatenates f of each element into a big vector;
   *  - biggest(f), computes the max of f of each element;
   *  - smallest(f), computes the min of f of each element;
   *  ... and many more...
   */

  object OpG {


  /** Find elements that minimize a function.
   *
   *  @param f is the function to be minimized.
   *  @return the minimum value and elements on this iterator that
   *          minimize [[f]].
   */

    def minimize[C](f: C => Double): Sri[C, (Double, Vector[C])] = 
      minimizef[C, C](f)(x => x) 


  /** Find elements that minimize a function; then do something to them.
   *
   *  @param f is the function to be minimize.
   *  @param g is the "something" to be done to the elements found.
   *  @return the minimum value and the transformed elements found. 
   */

    def minimizef[C, E]
      (f: C => Double)
      (g: C => E)
    : Sri[C, (Double, Vector[E])] = sri[C, (Double, E), (Double, Vector[E])](

      () => (Double.MaxValue, Vector[E]()),
      x => (f(x), g(x)),
      { case (x, y) => 
          if (x._1 < y._1) x 
          else if (x._1 == y._1) (x._1, x._2 :+ y._2)
          else (y._1, Vector[E](y._2))
      }
    )

           
  /** Find elements that maximize a function,
   *
   *  @param f is the function to be maximized.
   *  @return the maximum value and elements on this iterator that
   *          maximized [[f]].
   */

    def maximize[C](f: C => Double): Sri[C, (Double, Vector[C])] = 
      maximizef[C, C](f)(x => x)
           

  /** Find elements that maximize a function; then do something to them.
   *
   *  @param f is the function to be maximized.
   *  @param g is the "something" to be done to the elements found.
   *  @return the maximum value and transformed elements found.
   */

    def maximizef[C, E]
      (f: C => Double)
      (g: C => E)
    : Sri[C, (Double, Vector[E])] = sri[C, (Double, E), (Double, Vector[E])](

      () => (Double.MinValue, Vector[E]()),
      x => (f(x), g(x)),
      { case (x, y) => 
          if (x._1 > y._1) x 
          else if (x._1 == y._1) (x._1, x._2 :+ y._2)
          else (y._1, Vector[E](y._2))
      }
    )



  /** Apply a function to each element in this iterator.
   *
   *  @param f is the function to be applied.
   *  @return map(f)
   */

    def map[C, D](f: C => D): Sri[C, Vector[D]] = sri[C, D, Vector[D]](
      () => Vector[D](),
      f,
      { case (x,y) => x :+ y }
    )


  /** Apply a function to each element in this iterator; 
   *  then take big union.
   *
   *  @param f is the function to be applied.
   *  @return ext(f)
   */

    def ext[C, D](f: C => Vector[D]): Sri[C, Vector[D]] =
      sri[C, Vector[D], Vector[D]](
        () => Vector[D](),
        f,
        { case (x,y) => x ++ y }
      )



  /** Select elements in the iterator satisfying a predicate.
   *
   *  @param f is the predicate to be satisfied.
   *  @return elements satisfying [[f]].
   */

    def filter[C](f: C => Boolean): Sri[C, Vector[C]] = 
      ext[C, C](x => if (f(x)) Vector[C](x) else Vector[C]())


  /** Keep all elements from the iterator in a vector.
   */

    def keep[C]: Sri[C, Vector[C]] = sri[C, C, Vector[C]](
      () => Vector[C](),
      y => y,
      { case (x,y) => x :+ y }
    )


  /** Take average of f(x) for each x in the iterator.
   *
   *  @param f
   *  @return average value of f in the iterator.
   */

    def average[C](f: C => Double): Sri[C, Double] = 
      combine[C, Double, Double, Double](sum(f), count){
        case (x, y) => if (y != 0) x / y else 0
      }


  /** Make histogram of values of f in the iterator.
   *
   *  @param f
   *  @return the histogram of values of f.
   */

    def histogram[C, D](f: C => D): Sri[C, Map[D, Double]] =
      sri[C, D, Map[D, Double]](
        () => Map[D, Double](),
        f,
        { case (x, y) =>
            if (x contains y) { x += (y -> (x(y) + 1)) }
            else { x += (y -> 1) }
        }
      )
 


  /** Sum the values of f in this iterator.
   *
   *  @param f
   *  @return the sum of values of f.
   */

    def sum[C](f: C => Double): Sri[C, Double] = sri[C, Double, Double](
      () => 0,
      f,
      { case (x,y) => x + y }
    )



  /** Count the number of items on this iterator.
   *
   *  @return cardinality of this iterator.
   */

    def count[C]: Sri[C, Double] = sum[C](_ => 1:Double)



  /** Stats is a structure representing various statistics values.
   */

    case class Stats(
      average: Double, 
      variance: Double, 
      count: Double,
      sum: Double, 
      sumsq: Double, 
      min: Double, 
      max: Double) {

      override def toString(): String =
        s"Stats(average: ${average}, variance: ${variance}, " +
        s"count: ${count}, sum: ${sum}, sumsq: ${sumsq}, " +
        s"min: ${min}, max: ${max})"
    }



  /** Statistics on values of f in this iterator.
   *
   *  @param f
   *  @return statistics on values of f.
   */

    def stats[C](f: C => Double): Sri[C, Stats] = {

      def sq(x: C) = { val fx = f(x); fx * fx }

      def mix(es: Array[Double]) =
      {
        val cnt = es(0)
        val sum = es(1)
        val ssq = es(2)
        val min = es(3)
        val max = es(4)
        val ave = if (cnt != 0) sum/cnt else 0
        val vre = if (cnt>1) (ssq+(cnt*ave*ave)-(2*sum*ave))/(cnt-1) else 0

        Stats(
          average = ave, variance = vre, count = cnt, 
          sum = sum, sumsq = ssq, min = min, max = max
        )
      }  
 
      combineN(count[C], sum(f), sum(sq(_)), smallest(f), biggest(f))(mix _)
    }



  /** Concatenate values of f in this iterator.
   *
   *  @param f
   *  @return big union of applying f to elements on this iterator.
   */

    def concat[C, D](f: C => Vector[D]): Sri[C, Vector[D]] = ext(f)


  /** Smallest value of f in this iterator.
   *
   *  @param f
   *  @return smallest value of f.
   */

    def smallest[C](f: C => Double): Sri[C, Double] = sri[C, Double, Double](
      () => Double.MaxValue,
      f,
      { case (x,y) => x min y }
    )
      


  /** Biggest value of f in this iterator.
   *
   *  @param f
   *  @return biggest value of f.
   */

    def biggest[C](f: C => Double): Sri[C, Double] = sri[C, Double, Double](
      () => Double.MinValue,
      f,
      { case (x,y) => x max y }
    )
      


  /** Check whether all elements in this iterator satisfy a predicate.
   *
   *  @param f is the predicate to be satisfied.
   *  @return true iff f is satisfied by all elements.
   */

    def forall[C](f: C => Boolean): Sri[C, Boolean] = sri[C, Boolean, Boolean](
      () => true,
      f,
      { case (x, y) => x && y }
    )



  /** Check whether some elements in this iterator satisfy a predicate.
   *
   *  @param f is the predicate to be satisfied.
   *  @return true iff f is satisfied by some elements.
   */

    def thereis[C](f: C => Boolean): Sri[C, Boolean] = sri[C, Boolean, Boolean](
      () => false,
      f,
      { case (x, y) => x || y }
    )

  } // End object OpG




  /** Constructor for "Aggregate" iterators.
   *
   *  Construct iterators endowed with aggregate functions;
   *  i.e. iterators having the Aggregates trait.
   */

  object AggrIterator {


  /** Construct Aggregate iterator from ordinary iterator.
   *
   *  @param it is the ordinary iterator
   *  @return the Aggregate iterator constructed.
   */

    def fromIterator[C](it: Iterator[C]): AggrIterator[C] = 
      new Iterator[C] with Aggregates[C] {
        override def itC = it
        override def hasNext = it.hasNext
        override def next() = it.next() }


    def apply[C](it: Iterator[C]): AggrIterator[C] = fromIterator(it) 


  /** Construct Aggregate iterator from a list.
   *
   *  @param it is the list
   *  @return the Aggregate iterator constructed.
   */

    def apply[C](it: List[C]): AggrIterator[C] = fromIterator(it.iterator)



  /** Construct Aggregate iterator from a vector.
   *
   *  @param it is the vector.
   *  @return the Aggregate iterator constructed.
   */

    def apply[C](it: Vector[C]): AggrIterator[C] = fromIterator(it.iterator)



  /** Construct Aggregate iterator from a set.
   *
   *  @param it is the set.
   *  @return the Aggregate iterator constructed.
   */

    def apply[C](it: Set[C]): AggrIterator[C] = fromIterator(it.iterator)


  /** Construct Aggregate iterator from an arraybuffer.
   *
   *  @param it is the arraybuffer.
   *  @return the Aggregate iterator constructed.
   */

    def apply[C](it: ArrayBuffer[C]): AggrIterator[C] = 
      fromIterator(it.iterator)


  /** Construct Aggregate iterator from a map.
   *
   *  @param it is the map
   *  @return the Aggregate iterator constructed.
   */

    def apply[K, C](it: Map[K, C]): AggrIterator[(K, C)] =
      fromIterator(it.iterator)



  /** Group elements in the iterator by a grouping key; 
   *  compute an aggregate function on each group.
   *
   *  @param it is the iterator.
   *  @param group defines the grouping key.
   *  @param aggr is the aggregate function.
   *  @return a map from grouping key to its aggregate value.
   */ 

    def groupby[C, D, F]
      (it: Iterator[C])
      (group: C => D)
      (aggr: Sri[C, F])
    : Map[D, F] = aggr match { case Sri3(zero, iter, comb, done) =>

      val groups = Map[D,Any]()
      val ccomb = comb.asInstanceOf[(Any,Any)=>Any]
      val cdone = done.asInstanceOf[Any=>F]
 
      for (c <- it) {
        val k = group(c)
        if (groups contains k) { groups put (k,ccomb(groups(k),iter(c))) }
        else { groups put (k, ccomb(zero(), iter(c))) }
      }

      for ((k,x) <- groups) yield (k, cdone(groups(k)))  
    } 


  /** Partition elements in the iterator by a grouping key; 
   *  compute an aggregate function on each group.
   *
   *  partitionby(it)(f)(aggr) splits the iterator it at each
   *  position i where f(xi) and f(xi+1) differ. Then applies
   *  the aggregate function aggr to each chunk.
   *
   *  @param it is the iterator.
   *  @param group defines the grouping key.
   *  @param aggr is the aggregate function.
   *  @return an iterator of grouping key-aggregate value pairs.
   */ 

    def partitionby[C, D, F]
      (it: Iterator[C])
      (group: C => D)
      (aggr: Sri[C, F])
    : Iterator[(D, F)] = aggr match {

      case Sri3(zero, iter, comb, done) => new Iterator[(D, F)] {

        val itc = (for (c <- it) yield (group(c), iter(c))).buffered

        override def hasNext = itc.hasNext

        override def next() = {
          var gd = itc.next()
          var ac = comb(zero(), gd._2)
          while (itc.hasNext && itc.head._1 == gd._1) {
            gd = itc.next()
            ac = comb(ac,gd._2)
          }
          (gd._1, done(ac))
        }
      }
    }



  /** Apply an aggregate function on an iterator.
   *
   *  @param it is the iterator.
   *  @param aggr is the aggregate function.
   *  @return result of the aggregate; or Vector() if the iterator is empty
   */

    def aggregateBy[C, F](it: Iterator[C])(aggr:Sri[C, F]): Vector[F] =
      if (it.hasNext) Vector(Sri(aggr)(it)) else Vector()



  /** Apply an aggregate function on an iterator.
   *
   *  @param it is the iterator.
   *  @param aggr is the aggregate function.
   *  @return result of the aggregate.
   *
   *  Actually, flatAggregateBy(its)(aggr) is redundant.
   *  You can directly do aggr(it) instead.
   */

    def flatAggregateBy[C, F](it: Iterator[C])(aggr: Sri[C, F]): F = aggr(it)

  } // End object AggrIterator



  /** Implicit conversions of various collection types into
   *  AggrIterators.
   */

  object implicits {

    import scala.language.implicitConversions

    implicit def Iter2Aggr[C](it: Iterator[C]): AggrIterator[C] =
      AggrIterator(it)

    implicit def List2Aggr[C](it: List[C]): AggrIterator[C] = AggrIterator(it)

    implicit def Vec2Aggr[C](it: Vector[C]): AggrIterator[C] = AggrIterator(it)

    implicit def Set2Aggr[C](it: Set[C]): AggrIterator[C] = AggrIterator(it)

    implicit def Map2Aggr[K, C](it: Map[K, C]):AggrIterator[(K,C)] =
      AggrIterator(it)

    implicit def ArrayBuf2Aggr[C](it: ArrayBuffer[C]): AggrIterator[C] =
      AggrIterator(it)
  }  

} // End AggrCollections



/**
 *  Examples
 *

    {{{

import synchrony.iterators.AggrCollections._
import synchrony.iterators.AggrCollections.OpG._
import synchrony.iterators.AggrCollections.implicits._
import synchrony.programming.Sri._

val a = List((1,2),(1,3),(4,5),(5,6),(5,7),(5,8),(8,9))
def b = AggrIterator(a)

b.aggregateBy(histogram(_._1))
b.aggregateBy(minimize(_._1))
b.aggregateBy(maximize(_._1))
b.aggregateBy(stats(_._1))

def b1 = for (x <- b) yield x._1
def b2 = for (x <- b) yield x._2

b1.aggregateBy(biggest(x=>x))
b2.filter(_ > 5).aggregateBy(smallest(x => x))
b2.filter(_ > 11).aggregateBy(smallest(x => x))
b2.aggregateBy(withSlidingN(2)(sum {case(x,y) => x(0) + x(1) + y}))
b2.aggregateBy(withAcc(withSlidingN(2)(sum {case(x,y) => x(0) + x(1) + y})))

b.groups(_._1)
b.groupby(_._1)(keep)
b.groupby(_._1)(biggest(_._2))
b.groupby(_._1)(smallest(_._2))
b.groupby(_._1)(average(_._2))
b.groupby(_._1)(sum(_._2))
b.filter(_._2 > 5).groupby(_._1)(count)

b.aggregateBy(withAcc(smallest(_._2)))
b.aggregateBy(withAcc(count))
b.aggregateBy(withAcc(sum(_._2)))
b.aggregateBy(withAcc(average(_._2)))

b.groupby(_._1)(withAcc(average(_._2)))
b.partitionby(_._1)(withAccf((x:(Int,Int)) => x._2)(average(_._2)))
b.partitionby(_._1)(withAcc(average(_._2)))
b.partitionby(_._1)(average(_._2))

for ((x,y) <- b.groupby(_._1)(keep); z <- y) println(z)

 *
 * Here is an interesting example that combine a normal
 * aggregate function, count, which iterates an element
 * at a time; with a sliding-window aggregate function
 * which iterates 3 elements at a time. Both aggregates
 * are executed in a single loop (i.e. they dont iterate
 * seprately.)
 *

b2.aggregateBy(count + withSlidingN(2)(sum {case(x,y)=> x(0) + x(1) + y}))

    }}}

 *
 *
 */

