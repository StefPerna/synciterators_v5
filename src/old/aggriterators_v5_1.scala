
package synchrony
package iterators  

//
// Wong Limsoon
// 27/2/2020
//
// AggrCollections provide a trait, Aggregates, to
// endow iterators and other common collection types
// with groupby and common aggregate functions.
//


object AggrCollections {

  import scala.collection.mutable.ArrayBuffer
  import scala.collection.mutable.Map
  import scala.language.existentials
  import synchrony.programming.Sri
  import synchrony.programming.Sri._



  trait Aggregates[C]
  { //
    // The Aggregates trait endows its inheriting classes with
    // some commonly used aggregate functions and the groupby
    // operation.


    def itC: Iterator[C]
    //
    // itC is the only feature to be overriden in inheriting 
    // classes. itC is the iterator on which aggregate functions
    // are applied.


    final def groups[D](f:C=>D): Set[D] = 
    { //
      // groups(f) is the set of grouping keys.
      // This is mostly for info only.

      return (for(c <- itC) yield f(c)).toSet
    }


    final def groupby[K,F](f:C=>K)(aggr: Sri[C,F]): Map[K,F] =
    { //
      // groupby(f)(aggr) groups the elems on this iterator by f,
      // and then apply the aggregate function aggr on each group.
      // Actually, aggr is apply on the fly as each elem is inserted 
      // into its corresponding group.
      //
      // Must be named "groupby" because "groupBy" is a standard
      // method of scala collections.

      AggrIterator.groupby(itC, f, aggr)
    }



    final def partitionby[K,F](f:C=>K)(aggr: Sri[C,F]): Iterator[(K,F)] =
    { //
      // partitionby splits the iterator at each position i
      // where f(xi) and f(xi+1) differ. Then applies the
      // aggregate function aggr to each chunk. Actually it
      // applies aggr pn the fly as each xi is inserted into
      // its own chunk. It has the same output as groupby
      // when the iterator is sorted on f, but is much more
      // efficient than groupby.  
 
      AggrIterator.partitionby(itC, f, aggr)
    }


    final def aggregateBy[F](aggr: Sri[C,F]): Vector[F] =
    { //
      // aggregateBy applies the aggregate function aggr
      // on this iterator.

      AggrIterator.aggregateBy(itC, aggr)
    }


    final def flatAggregateBy[F](aggr: Sri[C,F]):F =
    {
      AggrIterator.flatAggregateBy(itC, aggr)
    }
  }



  object OpG 
  { //
    // Here are some commonly used "aggregate functions"
    // specified in terms of Sri:
    // - minimize(f), keep all elements which minimize f;
    // - maximize(f), keep elements which maximize f;
    // - map(f), applies f to each element;
    // - ext(f), flatmaps f to each element;
    // - keep, is basically map(x=>x);
    // - average(f), computes the average of f of each element;
    // - sum(f), computes the sum of f of each element;
    // - count,  counts the number of elements, basically it is sum(x=>1);
    // - concat(f), concatenates f of each element into a big vector;
    // - biggest(f), computes the max of f of each element;
    // - smallest(f), computes the min of f of each element;
    // - ... and many more...


    def minimize[C](f:C=>Double) = minimizef[C,C](x=>x, f) 

    def minimizef[C,E](g:C=>E, f:C=>Double) = 
      sri[C,(Double,E),(Double,Vector[E])](
        () => (Double.MaxValue, Vector[E]()),
        x => (f(x), g(x)),
        { case (x, y) => 
            if (x._1 < y._1) x else 
            if (x._1 == y._1) (x._1, x._2 :+ y._2) else 
               (y._1, Vector[E](y._2)) })

           
    def maximize[C](f:C=>Double) = maximizef[C,C](x=>x, f)
           
    def maximizef[C,E](g:C=>E, f:C=>Double) =
      sri[C,(Double,E),(Double,Vector[E])](
        () => (Double.MinValue, Vector[E]()),
        x => (f(x), g(x)),
        { case (x, y) => 
            if (x._1 > y._1) x else 
            if (x._1 == y._1) (x._1, x._2 :+ y._2) else 
               (y._1, Vector[E](y._2)) })


    def map[C,D](f:C=>D) = sri[C,D,Vector[D]](
      () => Vector[D](),
      f,
      {case (x,y) => x :+ y})


    def ext[C,D](f:C=>Vector[D]) = sri[C,Vector[D],Vector[D]](
      () => Vector[D](),
      f,
      {case (x,y) => x ++ y})


    def filter[C](f:C=>Boolean) = 
      ext[C,C](x => if (f(x)) Vector[C](x) else Vector[C]())


    def keep[C] = sri[C,C,Vector[C]](
      () => Vector[C](),
      y => y,
      {case (x,y) => x :+ y})


    def average[C](f:C=>Double) = 
      combine[C,Double,Double,Double](
        sum(f),
        count,
        {case (x, y) => if (y != 0) x / y else 0})


    def histogram[C,D](f:C=>D) = sri[C,D,Map[D,Double]](
      () => Map[D,Double](),
      f,
      {case (x,y)=> if (x contains y) {x +=(y->(x(y) + 1))} else {x +=(y->1)}})
 

    def sum[C](f:C=>Double) = sri[C,Double,Double](
      () => 0,
      f,
      {case (x,y) => x + y})


    def count[C] = sum[C](_ => 1:Double)


    case class Stats(
      average:Double, variance:Double, count:Double,
      sum:Double, sumsq: Double, min:Double, max:Double)
    {
      override def toString(): String =
        s"Stats(average: ${average}, variance: ${variance}, " +
        s"count: ${count}, sum: ${sum}, sumsq: ${sumsq}, " +
        s"min: ${min}, max: ${max})"
    }


    def stats[C](f:C=>Double) =
    {
      def sq(x:C) = { val fx = f(x); fx * fx }

      def mix(es:Array[Double]) =
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
          sum = sum, sumsq = ssq, min = min, max = max)
      }  
 
      combineN(mix _, count[C], sum(f), sum(sq(_)), smallest(f), biggest(f))
    }


    def concat[C,D](f:C=>Vector[D]) = sri[C,Vector[D],Vector[D]](
      () => Vector(),
      f,
      {case (x,y) => x ++ y})


    def smallest[C](f:C=>Double) = sri[C,Double,Double](
      () => Double.MaxValue,
      f,
      {case (x,y) => x min y})
      

    def biggest[C](f:C=>Double) = sri[C,Double,Double](
      () => Double.MinValue,
      f,
      {case (x,y) => x max y})
      

    def forall[C](f:C=>Boolean) = sri[C,Boolean,Boolean](
      () => true,
      f,
      {case (x,y) => x && y})


    def thereis[C](f:C=>Boolean) = sri[C,Boolean,Boolean](
      () => false,
      f,
      {case (x,y) => x || y})

  }



  type AggrIterator[C] = Iterator[C] with Aggregates[C]


  object AggrIterator
  { //
    // Functions for turning various kinds of collection types
    // into iterators with Aggregates trait.

    def build[C](it: Iterator[C]): AggrIterator[C] = 
      new Iterator[C] with Aggregates[C] {
        override val itC = it
        override def hasNext = it.hasNext
        override def next() = it.next() }

    def apply[C](it: Iterator[C]): AggrIterator[C] = build(it) 
    def apply[C](it: List[C]): AggrIterator[C] = build(it.iterator)
    def apply[C](it: Vector[C]): AggrIterator[C] = build(it.iterator)
    def apply[C](it: Set[C]): AggrIterator[C] = build(it.iterator)
    def apply[C](it: ArrayBuffer[C]): AggrIterator[C] = build(it.iterator)
    def apply[K,C](it: Map[K,C]): AggrIterator[(K,C)] = build(it.iterator)


    def groupby[C,D,F](
      it:Iterator[C], 
      group:C=>D, 
      aggr:Sri[C,F]):Map[D,F] =
    {
      aggr match { 
        case Sri3(zero, iter, comb, done) =>
        {
          val groups = Map[D,Any]()
          val ccomb = comb.asInstanceOf[(Any,Any)=>Any]
          val cdone = done.asInstanceOf[Any=>F]
 
          for (c <- it) {
            val k = group(c)
            if (groups contains k) { groups put (k,ccomb(groups(k),iter(c))) }
            else { groups put (k, ccomb(zero(), iter(c))) }
          }

          for ((k,x) <- groups) yield (k, cdone(groups(k)))  
    } } }


    def partitionby[C,D,F](
      it:Iterator[C],
      group:C=>D,
      aggr:Sri[C,F]):Iterator[(D,F)] =
    {
      aggr match { 
        case Sri3(zero, iter, comb, done) => new Iterator[(D,F)]
        {
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
    } } } }


    def aggregateBy[C,F](
      it:Iterator[C],
      aggr:Sri[C,F]):Vector[F] =
    {
      if (it.hasNext) Vector(Sri(aggr)(it)) else Vector()
    }


    def flatAggregateBy[C,F](
      it:Iterator[C], 
      aggr:Sri[C,F]): F =
    { //
      // Actually, flatAggregateBy(it, aggr) is redundant.
      // You can directly do aggr(it) instead.

      Sri(aggr)(it)
    }
  }



  object implicits
  { //
    // Implicit conversions of various collection types into
    // AggrIterators;c)C.e. endowing them with Aggregates trait
    // as and when needed via implicit conversion.

    import scala.language.implicitConversions

    implicit def Iter2Aggr[C](it:Iterator[C]):AggrIterator[C] = AggrIterator(it)

    implicit def List2Aggr[C](it:List[C]):AggrIterator[C] = AggrIterator(it)

    implicit def Vector2Aggr[C](it:Vector[C]):AggrIterator[C] = AggrIterator(it)

    implicit def Set2Aggr[C](it:Set[C]):AggrIterator[C] = AggrIterator(it)

    implicit def Map2Aggr[K,C](it:Map[K,C]):AggrIterator[(K,C)] =
      AggrIterator(it)

    implicit def ArrayBuf2Aggr[C](it:ArrayBuffer[C]):AggrIterator[C] =
      AggrIterator(it)
  }  

}



/*
 * Examples
 *

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

def b1= for(x <- b) yield x._1
def b2= for(x <- b) yield x._2

b1.aggregateBy(biggest(x=>x))
b2.filter(_ > 5).aggregateBy(smallest(x => x))
b2.filter(_ > 11).aggregateBy(smallest(x => x))
b2.aggregateBy(withSlidingN(2)(sum {case(x,y) => x(0) + x(1) + y}))
b2.aggregateBy(withAcc(withSliding2(sum {case(x,y) => x(0) + x(1) + y})))

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
b.partitionby(_._1)(withAccf(_._2, average(_._2)))
b.partitionby(_._1)(withAcc(average(_._2)))
b.partitionby(_._1)(average(_._2))

for((x,y) <- b.groupby(_._1)(keep); z <- y) println(z)

 *
 * Here is an interesting example that combine a normal
 * aggregate function, count, which iterates an element
 * at a time; with a sliding-window aggregate function
 * which iterates 3 elements at a time. Both aggregates
 * are executed in a single loop (i.e. they dont iterate
 * seprately.)
 *
b2.aggregateBy(count + withSlidingN(2)(sum {case(x,y)=> x(0) + x(1) + y}))
 *

 *
 */

