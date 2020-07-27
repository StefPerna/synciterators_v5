
package synchrony
package genomeannot

//
// Wong Limsoon
// 9/3/2020
//


object GenomeAnnot {

  import synchrony.iterators.SyncCollections._
  import synchrony.programming.Arith._

  case class GenomeError(msg: String) extends Throwable


  trait LocusLike
  {
    val chrom: String
    val chromStart: Int
    val chromEnd: Int

    def sameChrom(that: LocusLike) = GenomeLocus.SameChrom(this, that)
    def sameLocus(that: LocusLike) = GenomeLocus.SameLocus(this, that)


    def cond(ps:GenomeLocus.LocusPred*)(that:LocusLike) = {
      GenomeLocus.cond(ps:_*)(this,that)
    }

    val startBefore = cond(GenomeLocus.StartBefore) _
    val endBefore =  cond(GenomeLocus.EndBefore) _
    val startAfter = cond(GenomeLocus.StartAfter) _
    val endAfter = cond(GenomeLocus.EndAfter) _
    val startTogether = cond(GenomeLocus.StartTogether) _
    val endTogether = cond(GenomeLocus.EndTogether) _
    val inside = cond(GenomeLocus.Inside) _
    val enclose = cond(GenomeLocus.Enclose) _
    val outside = cond(GenomeLocus.Outside) _
    val touch = cond(GenomeLocus.Touch) _
    val disjoint = cond(GenomeLocus.DG(0)) _
    val nonDisjoint = cond(GenomeLocus.DLE(0)) _

    def overlap(n:Int = 1) = cond(GenomeLocus.Overlap(n)) _
    def near(n:Int = 1000) = cond(GenomeLocus.DLE(n)) _
    def farFrom(n:Int = 100000) = cond(GenomeLocus.DGE(n)) _

    def between(x:LocusLike, y:LocusLike) = 
      ((x endBefore this) && (this endBefore y)) ||
      ((y endBefore this) && (this endBefore x)) 

    def distFrom(that: LocusLike): Int = GenomeLocus.dist(this, that)
    def size = GenomeLocus.size(this)
    def canSee(n:Int = 1000)(that:LocusLike) = GenomeLocus.canSee(n)(this, that)
    def isBefore(that:LocusLike) = GenomeLocus.isBefore(this, that)

    def isBeforeByChromEnd(that:LocusLike) = 
      GenomeLocus.isBeforeByChromEnd(this, that)

    def isBeforeByChromStart(that:LocusLike) = 
      GenomeLocus.isBeforeByChromStart(this, that)
  }


  case class GenomeLocus(
    override val chrom: String,
    override val chromStart: Int,
    override val chromEnd: Int)
  extends LocusLike
  { //
    // Some operations on loci

    def intersect(that: LocusLike): Option[GenomeLocus] = {
      if (this nonDisjoint that) 
        Some(GenomeLocus(
          this.chrom,
          this.chromStart max that.chromStart,
          this.chromEnd min that.chromEnd)) 
       else None
    }

    def union(that: LocusLike): Option[GenomeLocus] = {
      if (this nonDisjoint that) 
        Some(GenomeLocus(
          this.chrom,
          this.chromStart min that.chromStart,
          this.chromEnd max that.chromEnd)) 
       else None
    }

    
    def +(dist:Int): GenomeLocus = {
      GenomeLocus(
        this.chrom,
        this.chromStart + dist,
        this.chromEnd + dist)
    }


    def -(dist:Int): GenomeLocus = {
      GenomeLocus(
        this.chrom,
        this.chromStart - dist,
        this.chromEnd - dist)
    }


    //
    // Start and end points represented as GenomeLocus,
    // so that locus-like operators can be re-used on them.

    def start = GenomeLocus(chrom, chromStart, chromStart)
    def end = GenomeLocus(chrom, chromEnd, chromEnd)
  }


  object GenomeLocus 
  {
    import synchrony.programming.StepContainer._
    import scala.collection.mutable.Queue
    import scala.collection.mutable.Queue._

    
    implicit def ordering[A<:LocusLike]: Ordering[A] = orderByChromStart
    //
    // It is possible to order locus based on chromEnd first,
    // or based on chromStart first. I choose chromStart first
    // here. You can change this to chromEnd first. But
    // whatever is chosen, the isBefore predicate should be
    // made consistent with this chosen. 

    def orderByChromEnd[A<:LocusLike]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromEnd, l.chromStart))

    def orderByChromStart[A<:LocusLike]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromStart, l.chromEnd))



    def SameChrom(x:LocusLike, y:LocusLike) = x.chrom == y.chrom


    def SameLocus(x:LocusLike, y:LocusLike) = 
      (x.chrom == y.chrom) && 
      (x.chromStart == y.chromStart) &&
      (x.chromEnd == y.chromEnd)


    def isBeforeByChromEnd(x:LocusLike, y:LocusLike) = orderByChromEnd.lt(x,y)

    def isBeforeByChromStart(x:LocusLike,y:LocusLike)=orderByChromStart.lt(x,y)

    def isBefore(x:LocusLike, y:LocusLike) = ordering.lt(x, y)
    //
    // make isBefore consistent with the default ordering


    def canSee(n:Int = 1000)(x:LocusLike, y:LocusLike) = cond(DLE(n))(x,y) 

      
    def dist(x:LocusLike, y:LocusLike): Int =
      if (x.chrom != y.chrom) Int.MaxValue else 
      if (x.chromEnd < y.chromStart) { y.chromStart - x.chromEnd } else 
      if (y.chromEnd < x.chromStart) { x.chromStart - y.chromEnd } 
      else 0

 
    def size(x:LocusLike) = x.chromEnd - x.chromStart


    def cond(ps:LocusPred*)(x: LocusLike, y:LocusLike) =
    { //
      // Verify loci x and y are on the same chromosome.
      // Then test if all predicates ps hold for them.

      (x.chrom == y.chrom) && ps.forall(p => p(x,y))
    }


    abstract class LocusPred
    { //
      // Various "topological predicates" on a pair of loci.
      // NOTE: These predicates assume they are testing
      // two loci on the same chromosome. 

      def apply(x:LocusLike, y:LocusLike):Boolean
      def and(that:LocusPred) = AndL(this, that)
      def or(that:LocusPred) = OrL(this, that)
    }


    case class AndL(u:LocusPred, v:LocusPred) extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = u(x,y) && v(x,y)
    }
       
    case class OrL(u:LocusPred, v:LocusPred) extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = u(x,y) || v(x,y)
    }


    case class GenPred(f:(LocusLike,LocusLike)=>Boolean) extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = f(x,y)
    }


    case object StartBefore extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = x.chromStart < y.chromStart
    }

    case object EndBefore extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = x.chromEnd < y.chromStart
    }
   
    case object StartAfter extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = x.chromStart > y.chromEnd
    }

    case object EndAfter extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = x.chromEnd > y.chromEnd
    }

    case object StartTogether extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = x.chromStart == y.chromStart
    }

    case object EndTogether extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = x.chromEnd == y.chromEnd
    }


    case object Inside extends LocusPred  {
      def apply(x:LocusLike, y:LocusLike) =
        (x.chromStart >= y.chromStart) && (y.chromEnd >= x.chromEnd)
    }

    case object Enclose extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = Inside(y, x)
    }

    case object Outside extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) = !Inside(x,y) && !Enclose(x,y)
    }

    case object Touch extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) =
        (x.chromEnd == y.chromStart) || (y.chromEnd == x.chromStart)
    }


    case class Overlap(n:Int) extends LocusPred {
      def apply(x:LocusLike, y:LocusLike) =
        (((y.chromStart <= x.chromStart) && (x.chromStart <= y.chromEnd)) ||
         ((x.chromStart <= y.chromStart) && (y.chromStart <= x.chromEnd))) &&
        ((x.chromEnd min y.chromEnd) - (x.chromStart max y.chromStart) >= n)
    }


    case class DL(n:Int) extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = dist(x,y) < n
    }

    case class DG(n:Int) extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = dist(x,y) > n
    }

    case class DLE(n:Int) extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = dist(x,y) <= n
    }

    case class DGE(n:Int) extends LocusPred {
      def apply(x: LocusLike, y:LocusLike) = dist(x,y) >= n
    }


    //
    // Some operations for merging loci

    def union(
      loci1: List[GenomeLocus], 
      loci2: List[GenomeLocus]) :List[GenomeLocus] =
    { //
      // Merge two sorted lists of loci. 
      // Loci on each list are non-overlapping in their own list.

      val q1 = loci1.to(Queue)
      val q2 = loci2.to(Queue)
      val qm = Queue(): Queue[GenomeLocus]
      val qa = Queue(): Queue[GenomeLocus]

      def more = (! q1.isEmpty) || (! q2.isEmpty) || (! qa.isEmpty)

      while (more) {

        Step0 {
          if (! qa.isEmpty) (qa.dequeue(), false) else
          if (q1.isEmpty) (q2.dequeue(), true) else
          if (q2.isEmpty) (q1.dequeue(), true) else
          if (q1.head.chromStart < q2.head.chromStart) (q1.dequeue(), true)
          else (q2.dequeue(), true) }

       .Step1 { case (acc, fresh) =>
          if (q1.isEmpty) (acc,fresh)
          else (acc union q1.head) match {
            case Some(locus) => { 
              q1.dequeue(); (locus, fresh || !(locus sameLocus acc)) }
            case None => (acc, fresh) } }
            
       .Step2 { case (acc, fresh) =>
          if (q2.isEmpty) (acc,fresh)
          else (acc union q2.head) match {
            case Some(locus) => {
              q2.dequeue(); (locus, fresh || !(locus sameLocus acc)) }
            case None => (acc, fresh) } }

       .Step3 { case (acc, fresh) =>
          if (fresh) qa.enqueue(acc) else qm.enqueue(acc) }

       .Done
      }

      return qm.to(List)
    }


    def unionAll(
      loci: List[List[GenomeLocus]]) :List[GenomeLocus] =
    { //
      // Given a list of list of loci. 
      // Merge overlapping loci.
   
      var acc = if (loci.isEmpty) Nil else loci.head
      var rest = if (loci.isEmpty) Nil else loci.tail

      while (! rest.isEmpty) {
         acc = union(acc, rest.head)
        rest = rest.tail
      }

      return acc
    }


    //
    // The "leftmost" and "rightmost" loci.

    val leftSentinel = GenomeLocus("", Int.MinValue, Int.MinValue)
    val rightSentinel = GenomeLocus("z", Int.MaxValue, Int.MaxValue)

  }
}


/*
 * Examples
 *

import synchrony.genomeannot.GenomeAnnot._

val l1 = GenomeLocus("1", 100, 200)
val l2 = GenomeLocus("1", 300, 400)
val l3 = GenomeLocus("1", 350, 600)
val l4 = GenomeLocus("1", 550, 700)
val l5 = GenomeLocus("1", 750, 900)
val l6 = GenomeLocus("2", 750, 900)
val l7 = GenomeLocus("2", 850, 950)
val l8 = GenomeLocus("3", 150, 950)

GenomeLocus.union(List(l1,l2,l3,l7),List(l4,l5,l6,l8))
GenomeLocus.union(List(l1,l2,l3,l4,l5,l6,l7,l8), List(l1,l2,l3,l4,l5,l6,l7,l8))
GenomeLocus.unionAll(List(List(l7,l8),List(l1,l2,l3),List(l4,l5,l6)))

 *
 */

/*
 * Expected output
 *

scala> GenomeLocus.union(List(l1,l2,l3,l7),List(l4,l5,l6,l8))
res0: List[synchrony.genomeannot.GenomeAnnot.GenomeLocus] = List(GenomeLocus(1,100,200), GenomeLocus(1,300,700), GenomeLocus(1,750,900), GenomeLocus(2,750,950), GenomeLocus(3,150,950))

scala> GenomeLocus.union(List(l1,l2,l3,l4,l5,l6,l7,l8), List(l1,l2,l3,l4,l5,l6,l7,l8))
res1: List[synchrony.genomeannot.GenomeAnnot.GenomeLocus] = List(GenomeLocus(1,100,200), GenomeLocus(1,300,700), GenomeLocus(1,750,900), GenomeLocus(2,750,950), GenomeLocus(3,150,950))

scala> GenomeLocus.unionAll(List(List(l7,l8),List(l1,l2,l3),List(l4,l5,l6)))
res2: List[synchrony.genomeannot.GenomeAnnot.GenomeLocus] = List(GenomeLocus(1,100,200), GenomeLocus(1,300,700), GenomeLocus(1,750,900), GenomeLocus(2,750,950), GenomeLocus(3,150,950))

 *
 */






