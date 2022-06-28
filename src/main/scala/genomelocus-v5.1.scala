
package gmql

/** Version 5
 *
 *  Wong Limsoon
 *  30 Jan 2022
 */


object GenomeLocus {


  import scala.language.implicitConversions


  type Bool   = Boolean                  // Shorthand
  type PRED   = (Locus, Locus) => Bool   // Shorthand


  case class Locus(chrom: String, start: Int, end: Int)
  {
    import Locus.dLocus

    /** These predicates are antimonotonic wrt isBefore.
     */

    def isBefore(u: Locus): Bool    = dLocus.isBefore(this, u)
    def sameChrom(u: Locus): Bool   = dLocus.sameChrom(this, u)
    def sameLocus(u: Locus): Bool   = this == u
    def sameStart(u: Locus): Bool   = dLocus.sameStart(this, u)
    def startBefore(u: Locus): Bool = dLocus.startBefore(this, u)
    def startAfter(u: Locus): Bool  = dLocus.startAfter(this, u)
    def overlapStart(u: Locus): Bool = dLocus.overlapStart(this, u)
    def overlap(n: Int = 1)(u: Locus): Bool = dLocus.overlap(n)(this, u)
    def near(n: Int = 1000)(u: Locus): Bool = dLocus.near(n)(this, u)


    /** These predicates are not antimonotonic wrt isBefore.
     */

    def sameEnd(u: Locus): Bool     = dLocus.sameEnd(this, u)
    def endBefore(u: Locus): Bool   = dLocus.endBefore(this, u)
    def endAfter(u: Locus): Bool    = dLocus.endAfter(this, u)
    def overlapEnd(u: Locus): Bool  = dLocus.overlapEnd(this, u)
    def inside(u: Locus): Bool      = dLocus.inside(this, u)
    def enclose(u: Locus): Bool     = dLocus.enclose(this, u)
    def outside(u: Locus): Bool     = dLocus.outside(this, u)
    def touch(u: Locus): Bool       = dLocus.touch(this, u)


    /** Other operations on loci.
     */

    def distFrom(u: Locus): Int     = dLocus.intdist(this, u)
    def +(n: Int): Locus            = Locus(chrom, start + n, end + n)
    def -(n: Int): Locus            = Locus(chrom, start - n, end - n)
    def chromStart: Locus           = Locus(chrom, start, start)
    def chromEnd: Locus             = Locus(chrom, end, end)


    def intersect(u: Locus): Option[Locus] = (this outside u) match {
      case true  => None
      case false => 
        val st = start max u.start
        val en = end min u.end
        Some(Locus(chrom, st, en))
    }


    def merge(u: Locus): Option[Locus] = (this outside u) match {
      case true  => None
      case false => 
        val st = start min u.start
        val en = end max u.end
        Some(Locus(chrom, st, en))
    }

  } // End case class Locus



  object Locus 
  {
    import dbmodel.DBModel.Predicates._

    
    /* Predicate framework for [[Locus]]
     */

    trait HasLocus[K] {
      def sameChrom:    (K,K) => Bool
      def isBefore:     (K,K) => Bool
      def sameLocus:    (K,K) => Bool
      def sameStart:    (K,K) => Bool
      def startBefore:  (K,K) => Bool
      def startAfter:   (K,K) => Bool
      def endBefore:    (K,K) => Bool
      def endAfter:     (K,K) => Bool
      def sameEnd:      (K,K) => Bool
      def overlapStart: (K,K) => Bool
      def overlapEnd:   (K,K) => Bool

// These are provided via [[Predicates.LINETOPO
//    
//    def overlap(n: Int): (K,K) => Bool
//    def near(n: Int):    (K,K) => Bool
//    def inside:      (K,K) => Bool
//    def enclose:     (K,K) => Bool
//    def outside:     (K,K) => Bool
//    def touch:       (K,K) => Bool
    }

    def isBefore[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.isBefore)(iHO.ord)
    }

    def sameChrom[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.sameChrom)(iHO.ord)
    }

    def sameLocus[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.sameLocus)(iHO.ord)
    }

    def sameStart[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.sameStart)(iHO.ord)
    }

    def startBefore[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.startBefore)(iHO.ord)
    }

    def startAfter[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.startAfter)(iHO.ord)
    }

    def overlapStart[K: HasLocus: HasOrder]: AntimonotonicPred[K] = {
      val iHL = implicitly[HasLocus[K]]
      val iHO = implicitly[HasOrder[K]]
      new AntimonotonicPred[K](iHL.overlapStart)(iHO.ord)
    }

    def endBefore[K: HasLocus]: GenPred[K,K] = {
      val iHL = implicitly[HasLocus[K]]
      new GenPred[K,K](iHL.endBefore)
    }

    def endAfter[K: HasLocus]: GenPred[K,K] = {
      val iHL = implicitly[HasLocus[K]]
      new GenPred[K,K](iHL.endAfter)
    }

    def sameEnd[K: HasLocus]: GenPred[K,K] = {
      val iHL = implicitly[HasLocus[K]]
      new GenPred[K,K](iHL.sameEnd)
    }

    def overlapEnd[K: HasLocus]: GenPred[K,K] = {
      val iHL = implicitly[HasLocus[K]]
      new GenPred[K,K](iHL.overlapEnd)
    }

      


    /** Set up the implicits for the predicates above to be used
     *  with [[DBModel.Join]].
     */

    trait DLOCUS extends HasOrder[Locus]
                 with HasDistance[Locus]
                 with HasSize[Locus]
                 with LINETOPO[Locus]
                 with HasLocus[Locus]


    implicit val dLocus = new DLOCUS 
    {
      /** The ordering on [[Locus]], which is the lexicographic
       *  ordering on [[(chrom, start, end)]].
       */

      val ord: Ordering[Locus]  = Ordering.by(Locus.unapply(_))

      /** Distance between two loci.
       */

      def intdist(v: Locus, u: Locus): Int = 
        if (v.chrom != v.chrom) Int.MaxValue
        else if (v.end < u.start) { u.start - v.end }
        else if (u.end < v.start) { v.start - u.end }
        else 0

      /** The size of a locus is its length
       */

      def sz(v: Locus): Double  = v.end - v.start

      /** These predicates are antimonotonic wrt isBefore
       */

      @inline def cond(p: PRED): PRED =           // Ensure (v, u) are on
        (v, u) => v.chrom == u.chrom && p(v, u)   // same chrom before testing
                                                  // for p(v, u).

      def sameChrom: PRED   = (v, u) => v.chrom == u.chrom
      def isBefore: PRED    = ord.lt _
      def sameLocus: PRED   = (v, u) => v == u
      def sameStart: PRED   = cond { (v, u) => v.start == u.start }
      def startBefore: PRED = cond { (v, u) => v.start < u.start }

      def overlapStart: PRED = cond { (v, u) => 
        v.start <= u.start && u.start <= v.end
      }

      def overlap(n: Int): PRED = cond { (v, u) => 
        v.end > u.start && u.end > v.start &&
        ((v.end min u.end) - (v.start max u.start) >= n)
      }

      def near(n: Int): PRED = cond { (v, u) => intdist(v, u) <= n }

      /** These predicates are not antimonotonic wrt isBefore
       */

      def startAfter: PRED  = cond { (v, u) => v.start > u.end } 
      def endBefore: PRED   = cond { (v, u) => v.end < u.start }
      def endAfter: PRED    = cond { (v, u) => v.end > u.end }
      def sameEnd: PRED     = cond { (v, u) => v.end == u.end }
      def touch: PRED = cond { (v, u) => v.end == u.start || u.end == v.start }
      def inside: PRED  = cond { (v, u) => u.start > v.start && v.end < u.end }
      def enclose: PRED = cond { (v, u) => v.start > u.start && u.end < v.end }
      override def outside: PRED = cond { (v, u) => 
        !sameChrom(v, u) || v.end < u.start || u.end < v.start
      }
      
      def overlapEnd: PRED = cond { (v, u) => 
        v.start <= u.end && u.end <= v.end
      }

      def far(n: Int): PRED = cond { (v, u) => intdist(v, u) >= n }
    }
      

    implicit val ordering: Ordering[Locus] = dLocus.ord



  }  // End object Locus

}  // End object GenomeLocus


