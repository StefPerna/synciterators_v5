
package synchrony.genomeannot

/** Define genomic locus and predicates on them.
 *
 *
 * Wong Limsoon
 * 17 June 2020
 */



object GenomeAnnot {

  import synchrony.iterators.SyncCollections._
  import synchrony.programming.Arith._

  case class GenomeError(msg: String) extends Throwable


  trait LocusLike {

    val chrom: String
    val chromStart: Int
    val chromEnd: Int

    def locus:GenomeLocus = GenomeLocus(chrom, chromStart, chromEnd)

    def sameChrom(that: LocusLike) = GenomeLocus.SameChrom(this, that)
    def sameLocus(that: LocusLike) = GenomeLocus.SameLocus(this, that)


    def cond(ps: GenomeLocus.LocusPred*)(that: LocusLike) = {
      GenomeLocus.cond(ps:_*)(this, that)
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

    def overlap(n: Int = 1) = cond(GenomeLocus.Overlap(n)) _
    def near(n: Int = 1000) = cond(GenomeLocus.DLE(n)) _
    def farFrom(n: Int = 100000) = cond(GenomeLocus.DGE(n)) _

    def between(x: LocusLike, y: LocusLike) = 
      ((x endBefore this) && (this endBefore y)) ||
      ((y endBefore this) && (this endBefore x)) 

    def distFrom(that: LocusLike): Int = GenomeLocus.dist(this, that)
    def size = GenomeLocus.size(this)

    def canSee(n: Int = 1000)(that: LocusLike) =
      GenomeLocus.canSee(n)(this, that)

    def isBefore(that: LocusLike) = GenomeLocus.isBefore(this, that)

    def isBeforeByChromEnd(that: LocusLike) = 
      GenomeLocus.isBeforeByChromEnd(this, that)

    def isBeforeByChromStart(that: LocusLike) = 
      GenomeLocus.isBeforeByChromStart(this, that)

  }  // End trait LocusLike


  case class GenomeLocus(
    override val chrom: String,
    override val chromStart: Int,
    override val chromEnd: Int
  ) extends LocusLike { 

  /** Intersect this GenomeLocus with another GenomeLocus, if they overlap.
   *
   *  @param that is the other GenomeLocus.
   *  @return Some(the intersection) or None.
   */

    def intersect(that: LocusLike): Option[GenomeLocus] = 
      (this nonDisjoint that) match { 
         case true  => 
           val st = this.chromStart max that.chromStart
           val en = this.chromEnd min that.chromEnd
           val ch = this.chrom
           (st == en) match {
              case true  => None
              case false => Some(GenomeLocus(ch, st, en))
         }
         case false => None
    }
    

  /** Merge this GenomeLocus with another GenomeLocus, if they overlap.
   *
   *  @param that is the other GenomeLocus.
   *  @return Some(the merged GenomeLocus) or None.
   */
  
    def union(that: LocusLike): Option[GenomeLocus] = 
      if (this nonDisjoint that) 
        Some(GenomeLocus(
          this.chrom,
          this.chromStart min that.chromStart,
          this.chromEnd max that.chromEnd)) 
       else None
    

    
  /** Relocate this GenomeLocus to the right (+) or to the left (-).
   *
   *  @param dist is the # of bp to move this GenomeLocus.
   *  @return the relocated GenomeLocus.
   */

    def +(dist: Int): GenomeLocus = 
      GenomeLocus(this.chrom, this.chromStart + dist, this.chromEnd + dist)
    

    def -(dist: Int): GenomeLocus = 
      GenomeLocus(this.chrom, this.chromStart - dist, this.chromEnd - dist)
    

  /** Start and end points represented as GenomeLocus,
   *  so that locus-like operators can be re-used on them.
   */

    def start = GenomeLocus(chrom, chromStart, chromStart)
    def end = GenomeLocus(chrom, chromEnd, chromEnd)

  }  // End of class GenomeLocus.



  /** Implementation of various topological predicates and
   *  other operations on loci.
   */

  object GenomeLocus {

    import synchrony.programming.StepContainer._
    import scala.collection.mutable.Queue
    import scala.collection.mutable.Queue._

    

  /** Positional orderings on loci.
   *
   *  It is possible to order locus based on chromEnd first,
   *  or based on chromStart first. I choose chromStart first
   *  here. You can change this to chromEnd first. But
   *  whatever is chosen, the isBefore predicate should be
   *  made consistent with this chosen. 
   */

    implicit def ordering[A<:LocusLike]: Ordering[A] = orderByChromStart

    def orderByChromEnd[A<:LocusLike]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromEnd, l.chromStart))

    def orderByChromStart[A<:LocusLike]: Ordering[A] =
      Ordering.by(l => (l.chrom, l.chromStart, l.chromEnd))



  /** Are two loci on the same chromosome?
   *
   *  @param x
   *  @param y are two loci.
   *  @return whether x and y are the same chromosome.
   */

    def SameChrom(x: LocusLike, y: LocusLike) = x.chrom == y.chrom


  /** Are two loci exactly the same?
   *
   *  @param x
   *  @param y are two loci.
   *  @return whether x and y are the same loci.
   */

    def SameLocus(x: LocusLike, y: LocusLike) = 
      (x.chrom == y.chrom) && 
      (x.chromStart == y.chromStart) &&
      (x.chromEnd == y.chromEnd)


  /** isBefore predicate based on ending positions.
   *
   *  @param x
   *  @param y are two loci.
   *  @return whether x ends before y.
   */
  
    def isBeforeByChromEnd(x: LocusLike, y: LocusLike) =
      orderByChromEnd.lt(x, y)


  /** isBefore predicate based on starting positions.
   *
   *  @param x
   *  @param y are two loci.
   *  @return whether x starts before y.
   */
  
    def isBeforeByChromStart(x: LocusLike,y: LocusLike) = 
      orderByChromStart.lt(x, y)


  /** Default isBefore predicate on loci for Synchrony iterators.
   *
   *  @param x
   *  @param y are two loci.
   *  @return whether x is before y according to the default
   *     positional ordering of loci.
   */

    def isBefore(x: LocusLike, y: LocusLike) = ordering.lt(x, y)


  /** Default canSee predicate on loci for Synchrony iterators.
   *
   *  @param n is the distance constraint.
   *  @param x
   *  @param y are two loci.
   *  @return whether x is no more than n bp from y.
   */

    def canSee(n: Int = 1000)(x: LocusLike, y: LocusLike) = cond(DLE(n))(x, y) 


      
  /** Distance between two loci.
   *
   *  @param x
   *  @param y are the two loci.
   *  @return the distance between (x, y).
   */

    def dist(x: LocusLike, y: LocusLike): Int =
      if (x.chrom != y.chrom) Int.MaxValue 
      else if (x.chromEnd < y.chromStart) { y.chromStart - x.chromEnd }
      else if (y.chromEnd < x.chromStart) { x.chromStart - y.chromEnd } 
      else 0



  /** Size of a loci.
   *
   *  @param x is the loci.
   *  @return its length.
   */

    def size(x: LocusLike) = x.chromEnd - x.chromStart


  /** Check whether a list of LocusPred all hold on a pair of loci.
   *
   *  @param ps is the list of LocusPred.
   *  @param x
   *  @param y are the two loci.
   *  @return whether (x, y) are on the same chromosome and
   *          all of ps hold on (x, y).
   */

    def cond(ps: LocusPred*)(x: LocusLike, y: LocusLike) = 
      (x.chrom == y.chrom) && ps.forall(p => p(x,y))

    

  /** Various "topological predicates" on a pair of loci.
   *
   *  NOTE: These predicates assume they are testing
   *  two loci on the same chromosome. 
   */

    abstract class LocusPred {

      // Logical combinators of LocusPred

      def apply(x: LocusLike, y: LocusLike):Boolean
      def and(that: LocusPred) = AndL(this, that)
      def or(that: LocusPred) = OrL(this, that)

      // Change this LocusPred to a point-based predicate.

      def basedOnStart = BasedOnStart(this)
      def basedOnEnd = BasedOnEnd(this)
    }


 
  /** Conjunction of two LocusPred.
   *
   *  @param u
   *  @param v are the two LocusPred.
   *  @return their conjunction.
   */

    case class AndL(u: LocusPred, v: LocusPred) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = u(x, y) && v(x, y)
    }
       

  /** Disjunction of two LocusPred.
   *
   *  @param u
   *  @param v are the two LocusPred.
   *  @return their disjunction.
   */

    case class OrL(u: LocusPred, v: LocusPred) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = u(x, y) || v(x, y)
    }



  /** Construct a LocusPred from a general Boolean function on loci.
   *
   *  @param f is the Boolean function on loci.
   *  @return the LocusPred constructed.
   */

    case class GenPred(f: (LocusLike, LocusLike) => Boolean) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = f(x, y)
    }


  /** Change a LocusPred to test only starting positions.
   *
   *  @param u is the LocusPred.
   *  @return a LocusPred p(x, y) such that p(x, y) iff u holds on
   *     on the starting positions of x and y.
   */
 
    case class BasedOnStart(u: LocusPred) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = u(x.locus.start, y.locus.start) 
    }



  /** Change a LocusPred to test only ending positions.
   *
   *  @param u is the LocusPred.
   *  @return a LocusPred p(x, y) such that p(x, y) iff u holds on
   *     on the ending positions of x and y.
   */
 
    case class BasedOnEnd(u: LocusPred) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = u(x.locus.end, y.locus.end)
    }



  /** Test some topological properties on two loci.
   *
   *  @param x
   *  @param y are the two loci.
   *  @return StartBefore(x, y) iff x starts before y.
   *          EndBefore(x, y) iff x ends before y.
   *          StartAfter(x, y) iff x starts after y.
   *          EndAfter(x, y) iff x ends after y.
   *          StartTogeter(x, y) iff x starts together with y.
   *          EndTogeter(x, y) iff x ends together with y.
   *          Inside(x, y) iff x is completely inside y.
   *          Enclose(x, y) iff y is completely inside x.
   *          Outside(x, y) iff x is completely outside y.
   *          Touch(x, y) iff x touches but does not overlap y.
   */

    case object StartBefore extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromStart < y.chromStart
    }

    case object EndBefore extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromEnd < y.chromStart
    }
   
    case object StartAfter extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromStart > y.chromEnd
    }

    case object EndAfter extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromEnd > y.chromEnd
    }

    case object StartTogether extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromStart == y.chromStart
    }

    case object EndTogether extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = x.chromEnd == y.chromEnd
    }


    case object Inside extends LocusPred  {
      def apply(x: LocusLike, y: LocusLike) =
        (x.chromStart >= y.chromStart) && (y.chromEnd >= x.chromEnd)
    }

    case object Enclose extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = Inside(y, x)
    }

    case object Outside extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = !Inside(x,y) && !Enclose(x,y)
    }

    case object Touch extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) =
        (x.chromEnd == y.chromStart) || (y.chromEnd == x.chromStart)
    }


  /** Test whether two loci overlap by at least n bp.
   *
   *  @param n is the distance constraint.
   *  @param x
   *  @param y are the two loci.
   *  @return Overlap(n)(x, y) iff x overlaps y at least n bp.
   */

    case class Overlap(n: Int) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) =
        (((y.chromStart <= x.chromStart) && (x.chromStart <= y.chromEnd)) ||
         ((x.chromStart <= y.chromStart) && (y.chromStart <= x.chromEnd))) &&
        ((x.chromEnd min y.chromEnd) - (x.chromStart max y.chromStart) >= n)
    }



  /** Test distance contraints between two loci.
   *
   *  @param n is the distance limit.
   *  @param x
   *  @param y are the two loci.
   *  @return  DL(n)(x, y) iff x is less than n bp from y.
   *           DG(n)(x, y) iff x is more than n bp from y.
   *          DLE(n)(x, y) iff x is no more than n bp from y.
   *          GLE(n)(x, y) iff x is at least n bp from y.
   */

    case class DL(n: Int) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = dist(x,y) < n
    }

    case class DG(n: Int) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = dist(x,y) > n
    }

    case class DLE(n: Int) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = dist(x,y) <= n
    }

    case class DGE(n: Int) extends LocusPred {
      def apply(x: LocusLike, y: LocusLike) = dist(x,y) >= n
    }


    //
    // The "leftmost" and "rightmost" loci.
    //

    val leftSentinel = GenomeLocus("", Int.MinValue, Int.MinValue)
    val rightSentinel = GenomeLocus("z", Int.MaxValue, Int.MaxValue)

  }  // End object GenomeLocus
}




