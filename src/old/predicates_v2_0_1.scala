
package synchrony.gmql


/** GenoMetric Query Language (GMQL)
 *
 *  Implement GMQL, a domain-specigic language for querying Bed-like files.
 *  This module provides support for specifying genometric predicates.
 *
 *  The three kinds of genometric predicates are: SBPred, BPred, SPred.
 *
 *  SBPred(s, u) is a predicate on a Sample s and a Bed entry u.
 *  BPred(u) is a predicate on a Bed entry u.
 *  SPred(s) is a predicate on a Sample s.
 *
 *
 * Wong Limsoon
 * 29/4/2020
 */




object Predicates {

  import synchrony.gmql.Samples
// import synchrony.gmql.GMQL.DB
  import synchrony.genomeannot.GenomeAnnot._
  import synchrony.programming.Arith._


  type Bed = Samples.Bed
  type Sample = Samples.Sample



  case class PredicateError(msg: String) extends Throwable


  /** @param ps is a list of SBPred
   *  @param s is a Sample
   *  @param u is a Bed entry.
   *  @return whether all the SBPred in ps hold on (s, u).
   */

  def condSB(ps:SBPred*)(s:Sample, u:Bed) = ps.forall(p => p(s,u))


  /** @param ps is a list of BPred
   *  @param u is a Bed entry.
   *  @return whether all the BPred in ps hold on u.
   */

  def condB(ps:BPred*)(u:Bed) = ps.forall(p => p(u))


  /** @param ps is a list of SPred
   *  @param s is a Sample
   *  @return whether all the SPred in ps hold on s.
   */

  def condSP(ps:SPred*)(s:Sample) = ps.forall(p => p(s))


  //
  // SBPred, predicate on a sample and a locus on its track.
  //
  // The abstract syntax for SBPred is
  //
  // SBPred ::= SBPred and SBPred
  //         |  SBPred or SBPred
  //         |  not SBPred
  //         |  GenSBPred(f: (Sample, Bed) => Boolean)
  //         |  MkSBPred(f: Sample => BPred)
  //         |  SBObj cop SBObj | SObj cop BObj | BObj cop SObj
  //         |  SPred | BPred
  //
  // SBObj  ::= SBObj aop SBObj | SObj aop BObj | BObj aop SObj 
  //         | GenSBObj[A](f: (Sample, Bed) => A)
  //         | MkSBObj[A<:AnyVal](a:A) | MkStringSBObj[a:String]
  //
  // cop    ::= === | !== | >= | > | <= | <
  // aop    ::= + | - | * | /
  //

  /** Logical combinators---AndSB, OrSB, NotSB---for combining SBPred.
   *
   *  @param su
   *  @param sv are SBPred
   *  @return the conjunction, disjunction, and negation of these SBPred.
   */

  abstract class SBPred {
    def apply(s: Sample, u: Bed): Boolean
    def and(that: SBPred): SBPred = AndSB(this, that)
    def or(that: SBPred): SBPred = OrSB(this, that)
    def not: SBPred = NotSB(this)
  }

  case class AndSB(su: SBPred, sv: SBPred) extends SBPred {
    def apply(s: Sample, u: Bed) = su(s,u) && sv(s,u)
  } 
    
  case class OrSB(su: SBPred, sv: SBPred) extends SBPred {
    def apply(s: Sample, u: Bed) = su(s,u) || sv(s,u)
  } 
    
  case class NotSB(su: SBPred) extends SBPred {
    def apply(s: Sample, u: Bed) = !su(s,u)
  }


  /** Combinator to wrap a function (Sample, Bed) => Boolean into a SBPred.
   *
   *  This combinator is a basically a way to "declare" an arbitrary
   *  function of type (Sample, Bed) => Boolean as a SBPred.
   *
   *  @param f is the function.
   *  @return the SBPred.
   */

  implicit class GenSBPred(f: (Sample, Bed) => Boolean) extends SBPred {
    def apply(s: Sample, u: Bed) = f(s,u)
  }


  /** Combinator to wrap a function Sample => BPred into a SBPred.
   *
   *  Note that a BPred is equiv to a function Bed => Boolean.
   *  Thus  this combinator essentially wraps a function of type
   *  Sample => Bed => Boolean into a SBPred.
   *  
   *  @param f is the function
   *  @return the SBPred.
   */

  implicit class MkSBPred(f: Sample => BPred) extends SBPred {
    def apply(s: Sample, u: Bed) = f(s)(u)
  }



  //
  // BPred, predicate on a Bed entry.
  //
  // The abstract syntax for BPred is:
  //
  // BPred ::= BPred and BPred
  //        |  BPred or BPred
  //        |  not BPred
  //        |  GenBPred(f: Bed => Boolean)
  //        |  BObj cop BObj
  //
  // BObj  ::= Chr | Start | End | Strand | Score | Name
  //        | GenBObj[A](f: Bed => A) | MetaR[A](k: String)
  //        | MkBObj[A<:AnyVal](a:A) | MkStringBObj[a:String]
  //        | BObj aop Bobj 
  //
  // cop   ::= === | !== | < | <= | > | >= 
  // aop   ::=  + | - | * | /
  //

  /** Logical combinators---AndB, OrB, NotB---for combining SBPred.
   *
   *  @param u
   *  @param v are BPred
   *  @return the conjunction, disjunction, and negation of these BPred.
   */

  abstract class BPred {

    def apply(u: Bed): Boolean
    def and(that: BPred): BPred = AndB(this, that)
    def or(that: BPred): BPred = OrB(this, that)
    def not: BPred = NotB(this)
  }
  
  case class NotB(u: BPred) extends BPred {
    def apply(x: Bed) = !u(x)
  }

  case class AndB(u: BPred, v: BPred) extends BPred {
    def apply(x: Bed) = u(x) && v(x)
  }
      
  case class OrB(u: BPred, v: BPred) extends BPred {
    def apply(x: Bed) = u(x) || v(x)
  }


  /** Combinator to wrap a function Bed => Boolean into a BPred.
   *
   *  This combinator is a basically a way to "declare" an arbitrary
   *  function of type Bed => Boolean as a BPred.
   *
   *  @param f is the function.
   *  @return the BPred.
   */

  implicit class GenBPred(f: Bed => Boolean) extends BPred {
    def apply(x: Bed) = f(x)
  }



  //
  // SPred, predicate on a Sample.
  //
  // The abstract syntax for SPred is:
  //
  // SPred ::= SPred and SPred
  //        |  SPred or SPred
  //        |  not SPred
  //        |  GenSPred(f: Sample => Boolean)
  //        |  SemiJoin(incl: Incl, excl: Excl, exDB: DB)
  //        |  SObj cop SObj
  //
  // Incl  ::= Incl(f: String*)
  // Excl  ::= Excl(f: String*)
  //
  // SObj  ::= SObj aop SObj | MetaS[A](k: String)
  //        | MkSObj[A<:AnyVal](a:A) | MkStringSObj[a:String]
  //
  // cop   ::= === | !== | >= | > | <= | <
  // aop   ::= + | - | * | /
  //


  /** Logical combinators---AndS, OrS, NotS---for combining SPred.
   *
   *  @param u
   *  @param v are SPred
   *  @return the conjunction, disjunction, and negation of these SPred.
   */

  abstract class SPred {

    def apply(s: Sample): Boolean
    def and(that: SPred): SPred = AndS(this, that)
    def or(that: SPred): SPred = OrS(this, that)
    def not: SPred = NotS(this)
  }

  case class AndS(u: SPred, v: SPred) extends SPred {
    def apply(s: Sample) = u(s) && v(s)
  }

  case class OrS(u: SPred, v: SPred) extends SPred {
    def apply(s: Sample) = u(s) || v(s)
  }

  case class NotS(u: SPred) extends SPred {
    def apply(s: Sample) = !u(s)
  }
 

  /** Combinator to wrap a function Sample => Boolean into an SPred.
   *
   *  This combinator is a basically a way to "declare" an arbitrary
   *  function of type Sample => Boolean as an SPred.
   *
   *  @param f is the function.
   *  @return the SPred.
   */

  implicit class GenSPred(f: Sample => Boolean) extends SPred {
    def apply(s: Sample) = f(s)
  }
      
  
  /** To implement/emulate GMQL, we also need to support
   *  a special kind of SPred called a SemiJoin.
   *
   *  SemiJoin(incl, excl, exDB)(s) holds on Sample s if
   *  all the Samples in exDB agree with Sample s on all meta
   *  attributes listed in incl, and disagree with s on all
   *  meta attributes listed in excl.
   *
   *  @param incl is the "must agree" list of meta attributes.
   *  @param excl is the "must disagree" list of meta attributes.
   *  @param exDB is a list of Samples to check against.
   *  @returm the SemiJoin predicate.
   */

  case class Incl(f: String*)

  case class Excl(f: String*)


  case class SemiJoin(
    incl: Incl = Incl(), 
    excl: Excl = Excl(), 
    exDB: Iterable[Sample]) 
  extends SPred {

    def apply(s: Sample) = {

      def agree(s: Sample, t: Sample) = { (l: String) => 
        s.checkM[Any](l, u => t.checkM[Any](l, v => u == v)) 
      }

      def disagree(s: Sample, t: Sample) = { (l: String) =>
        s.checkM[Any](l, u => t.checkM[Any](l, v => u != v, true)) 
      }

      exDB.exists { t =>
        incl.f.forall (agree(s, t)) &&
        excl.f.forall (disagree(s, t)) 
      }
    }

    def chk(s:Sample) = false
  }



  /** Combinator to build BObj, or Bed objects.
   *
   *  BObj represents a region attribute of a Bed entry. BObj 
   *  provides comparison and arithmetic operations on region
   *  attributes of Bed objects, for forming BPred. 
   *  Cf. abstract syntax of BPred.
   */

  abstract class BObj[A] {

  /** Instantiate a BObj. A Bobj is intended to represent the region
   *  attribute of a Bed entry. Instantiating a BObj means getting
   *  the value of that region attribute. Thus this.apply(u) or
   *  this(u) should give the value of this attribute of u, when
   *  "this" is a BObj (representing a region attribute.)
   *
   *  @param u is the Bed entry
   *  @return the value of the region attribute of u that this 
   *     BObj represents.
   */

    def apply(u: Bed): A


  /** @param u is a Bed entry.
   *  @param f is a predicate.
   *  @return whether the region attribute represented by this BObj
   *     has a value in u that satisfies f. I.e. return whether
   *     f(this(u)) holds, if this BObj is indeed an attribute of u;
   *     and is false if this BObj is not an attribute of u.
   */

    def chk(u: Bed, f: A => Boolean): Boolean


   /** Comparison operators on BObj.
    *
    *  @param m another BObj to compare with this BObj
    *  @return whether in the Bed entry being tested, this BObj has a
    *     value that is equal to, not equal to, greater than or equal 
    *     to, greater than, less than or equal to, less than the
    *     value the other BObj has.
    */  

    type BO  = BObj[A]
    type CMP = (A, A) => Boolean
    def cmp(ev: CMP, m: BO) = GenBPred(u => m.chk(u, x => chk(u, y => ev(y,x))))
 
    def ===(m: BO): BPred = GenBPred(u => m.chk(u, x => chk(u, _ == x)))
    def !==(m: BO): BPred = GenBPred(u => m.chk(u, x=> chk(u, _ != x)))
    def >=(m: BO)(implicit ev: Ordering[A]): BPred = cmp(ev.gteq, m)
    def >(m: BO)(implicit ev: Ordering[A]): BPred = cmp(ev.gt, m)
    def <=(m: BO)(implicit ev: Ordering[A]): BPred = cmp(ev.lteq, m)
    def <(m: BO)(implicit ev: Ordering[A]): BPred = cmp(ev.lt, m)


  /** Arithmetic operators on BObj.
   *
   *  @param m is another BObj.
   *  @return the value of this BObj +, -, *, / to the value of
   *     the BObj m, in a given Bed entry.
   */

    def +(m: BO)(implicit ev:Arith[A]): BO = ArithBO[A](ev.addA, this, m)
    def -(m: BO)(implicit ev:Arith[A]): BO = ArithBO[A](ev.subA, this, m)
    def *(m: BO)(implicit ev:Arith[A]): BO = ArithBO[A](ev.mulA, this, m)
    def /(m: BO)(implicit ev:Arith[A]): BO = ArithBO[A](ev.divA, this, m)
  }


  /** The chromosome, chromosome start, chromosome end,
   *  name, strand, and score of a Bed entry are BObj.
   */

  case object Chr extends BObj[String] {
    def apply(u: Bed) = u.chrom
    def chk(u: Bed, f: String => Boolean) = f(this(u))
  }

  case object Start extends BObj[Int] {
    def apply(u: Bed) = u.chromStart
    def chk(u: Bed, f: Int => Boolean) = f(this(u))
  } 

  case object End extends BObj[Int] {
    def apply(u: Bed) = u.chromEnd
    def chk(u: Bed, f: Int => Boolean) = f(this(u))
  }

  case object Name extends BObj[String] {
    def apply(u: Bed) = u.name
    def chk(u: Bed, f: String => Boolean) = f(this(u))
  }

  case object Strand extends BObj[String] {
    def apply(u: Bed) = u.strand
    def chk(u: Bed, f: String => Boolean) = f(this(u))
  }
  
  case object Score extends BObj[Double] {
    def apply(u: Bed) = u.score
    def chk(u: Bed, f: Double => Boolean) = f(this(u))
  }
  
  case class GenBObj[A](g: Bed => A) extends BObj[A] {
    def apply(u: Bed) = g(u)
    def chk(u: Bed, f: A => Boolean) = f(g(u))
  }

  
  /** Region attributes of Bed entries are BObj.
   */

  case class MetaR[A](k: String) extends BObj[A] {
    def apply(u: Bed) = u.getMisc[A](k)
    def chk(u: Bed, f: A => Boolean) = u.checkMisc(k, f)
  }



  /** Combinator to build SObj.
   *
   *  SObj represents a meta attribute of a Sample. SObj 
   *  provides comparison and arithmetic operations on 
   *  meta attributes of Samples, for forming SBPred and
   *  SPred.  Cf. abstract syntax of SPred.
   */

  abstract class SObj[A] { 


  /** This SObj is a meta attribute of a Sample s. Thus,
   *  this(s) or this.apply(s) give the value of this
   *  meta attribute in Sample s.
   *
   *  @param s is a sample
   *  @return the value of the meta attribute represented by 
   *     this SObj in Sample s.
   */

    def apply(s:Sample):A


  /** @param s is a sample
   *  @param f is a predicate
   *  @return whether in Sample s, the value of the meta attribute
   *     represented by this SObj satisfies the predicate f.
   *     Return false if this meta attribute is not found in s.
   */

    def chk(s: Sample, f: A => Boolean): Boolean


   /** Comparison operators on SObj.
    *
    *  @param m another SObj to compare with this SObj
    *  @return whether in the Sample being tested, this SObj has a
    *     value that is equal to, not equal to, greater than or equal 
    *     to, greater than, less than or equal to, less than the
    *     value the other SObj has.
    */  

    type SO  = SObj[A]
    type CMP = (A, A) => Boolean
    def cmp(ev: CMP, m: SO) = GenSPred(s => m.chk(s, x => chk(s, y => ev(y,x))))

    def ===(m: SO): SPred = GenSPred(s => m.chk(s, x => chk(s, _ == x)))
    def !==(m: SO): SPred = GenSPred(s => m.chk(s, x => chk(s, _ != x)))
    def >=(m: SO)(implicit ev: Ordering[A]): SPred = cmp(ev.gteq, m)
    def >(m: SO)(implicit ev: Ordering[A]): SPred = cmp(ev.gt, m)
    def <=(m: SO)(implicit ev: Ordering[A]): SPred = cmp(ev.lteq, m)
    def <(m: SO)(implicit ev: Ordering[A]): SPred = cmp(ev.lt, m)


  /** Arithmetic operators on SObj.
   *
   *  @param m is another SObj.
   *  @return the value of this SObj +, -, *, / to the value of
   *     the SObj m, in a given Sample.
   */

    def +(m: SO)(implicit ev: Arith[A]): SO = ArithSO[A](ev.addA, this, m)
    def -(m: SO)(implicit ev: Arith[A]): SO = ArithSO[A](ev.subA, this, m)
    def *(m: SO)(implicit ev: Arith[A]): SO = ArithSO[A](ev.mulA, this, m)
    def /(m: SO)(implicit ev: Arith[A]): SO = ArithSO[A](ev.divA, this, m)
  }


  /** Make a function Sample => A into an SObj. 
   *
   *  @param g is the function.
   *  @return the SObj constructed.
   */

  case class GenSObj[A](g: Sample => A) extends SObj[A] {
    def apply(s: Sample) = g(s)
    def chk(s: Sample, f: A => Boolean) = f(g(s))
  }

  
  /** Meta attribute of a Sample are SObj.
   */

  case class MetaS[A](k: String) extends SObj[A] {
    def apply(s: Sample) = s.getM[A](k)
    def chk(s: Sample, f: A => Boolean) = s.checkM(k, f)
  }



  /** Combinator to build SBObj.
   *
   *  SBObj represents a region attribute of a Bed entry of a Sample.
   *  Generally, SBObj is used when processing Bed entries of a Sample;
   *  but unlike BObj, you also want access to the whole Sample during
   *  this process. Cf. the abstract syntax of SBPred.
   */

  abstract class SBObj[A] { 

    def apply(s: Sample, u: Bed): A
    def chk(s: Sample, u: Bed, f: A => Boolean): Boolean

    // Here are the SBObj functions...

    type SBO = SBObj[A]
    type CMP = (A, A) => Boolean

    def cmp(ev: CMP, m: SBO) = GenSBPred(
      { case(s, u) => chk(s, u, y => m.chk(s, u, x => ev(y,x))) }
    )
    

   /** Comparison operators on SBObj.
    */

    def ===(m: SBO) = GenSBPred(
      { case(s, u) => chk(s, u, y => m.chk(s, u, x => y == x)) }
    )

    def !==(m: SBO) = GenSBPred(
      { case(s, u) => chk(s, u, y => m.chk(s, u, x => y != x)) }
    )

    def >=(m: SBO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gteq, m)
    def >(m: SBO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gt, m)
    def <=(m: SBO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lteq, m)
    def <(m: SBO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lt, m)


  /** Arithmetic operators on SBobj.
   */

    def +(m: SBO)(implicit ev: Arith[A]): SBO = ArithSBO[A](ev.addA, this, m)
    def -(m: SBO)(implicit ev: Arith[A]): SBO = ArithSBO[A](ev.subA, this, m)
    def *(m: SBO)(implicit ev: Arith[A]): SBO = ArithSBO[A](ev.mulA, this, m)
    def /(m: SBO)(implicit ev: Arith[A]): SBO = ArithSBO[A](ev.divA, this, m)
  }


  /** @param g is a function on a Sample and a Bed entry.
   *  @return the SBObj constructed from the function.
   */

  case class GenSBObj[A](g: (Sample, Bed) => A) extends SBObj[A] {
    def apply(s: Sample, u: Bed) = g(s, u)
    def chk(s: Sample, u: Bed, f: A => Boolean) = f(g(s,u))
  }

  
  
  /** Conversions of String, Int, etc. into SBObj, SObj and BObj. 
   */
   
  implicit class MkBObj[A<:AnyVal](val a: A) extends BObj[A] {
    def apply(u: Bed) = a
    def chk(u: Bed, f:A => Boolean) = f(a)
  }

  implicit class MkStringBObj(val a: String) extends BObj[String] {
    def apply(u: Bed) = a
    def chk(u: Bed, f: String => Boolean) = f(a)
  }

  implicit class MkSObj[A<:AnyVal](val a: A) extends SObj[A] {
    def apply(s: Sample) = a
    def chk(s: Sample, f: A => Boolean) = f(a)
  }

  implicit class MkStringSObj(val a: String) extends SObj[String] {
    def apply(s: Sample) = a
    def chk(s: Sample, f: String => Boolean) = f(a)
  }

  

  /** Lift SObj to SBObj. 
   *
   *  This is needed when an SObj is compared
   *  to a BObj or when an SObj is +, -, *, / to a BObj. E.g. when
   *  you compare a meta attribute of a Sample to a region attribute
   *  of a Bed entry of that Sample.
   */

  implicit class LiftS[A](m: SObj[A]) {

    type BO  = BObj[A]
    type SBO = SBObj[A]

    def cmp(ev: (A, A) => Boolean, n: BO) = GenSBPred(
      { case(s, u) => m.chk(s, y => n.chk(u, x => ev(y, x))) }
    ) 

    def ===(n: BO): SBPred = GenSBPred(
      { case(s, u) => m.chk(s, y => n.chk(u, x => y == x)) }
    )

    def !==(n: BO): SBPred = GenSBPred(
      { case(s, u) => m.chk(s, y => n.chk(u, x => y != x)) }
    )

    def >=(n: BO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gteq, n)
    def >(n: BO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gt, n)
    def <=(n: BO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lteq, n)
    def <(n: BO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lt, n)
    
    def +(n: BO)(implicit ev: Arith[A]): SBO = ArithSB[A](ev.addA, m, n)
    def -(n: BO)(implicit ev: Arith[A]): SBO = ArithSB[A](ev.subA, m, n)
    def *(n: BO)(implicit ev: Arith[A]): SBO = ArithSB[A](ev.mulA, m, n)
    def /(n: BO)(implicit ev: Arith[A]): SBO = ArithSB[A](ev.divA, m, n)
  }


  /** Lift BObj to SBObj. 
   *
   *  This is needed when a BObj is compared to an SObj or 
   *  when a BObj is +, -, *, / to an SObj. E.g. when you compare 
   *  a meta attribute of a Sample to a region attribute of a Bed 
   *  entry of that Sample.
   */

  implicit class LiftR[A](m: BObj[A]) {

    type SO = SObj[A]
    type SBO = SBObj[A]

    def cmp(ev: (A, A) => Boolean, n: SO) = GenSBPred(
      { case(s, u) => m.chk(u, y => n.chk(s, x => ev(y, x))) }
    ) 

    def ===(n: SO): SBPred = GenSBPred(
      { case(s, u) => m.chk(u, y => n.chk(s, x => y == x)) }
    )

    def !==(n: SO): SBPred = GenSBPred(
      { case(s, u) => m.chk(u, y => n.chk(s, x => y != x)) }
    )

    def >=(n: SO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gteq, n)
    def >(n: SO)(implicit ev: Ordering[A]): SBPred = cmp(ev.gt, n)
    def <=(n: SO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lteq, n)
    def <(n: SO)(implicit ev: Ordering[A]): SBPred = cmp(ev.lt, n)
    
    def +(n: SO)(implicit ev: Arith[A]): SBO = ArithBS[A](ev.addA, m, n)
    def -(n: SO)(implicit ev: Arith[A]): SBO = ArithBS[A](ev.subA, m, n)
    def *(n: SO)(implicit ev: Arith[A]): SBO = ArithBS[A](ev.mulA, m, n)
    def /(n: SO)(implicit ev: Arith[A]): SBO = ArithBS[A](ev.divA, m, n)
  }

  

  /** Lift SPred to SBPred.
   */

  implicit class S2SBPred(p: SPred) extends SBPred {
    def apply(s: Sample, u: Bed) = p(s)
  }


  /** Lift BPred to SBPred.
   */

  implicit class L2SBPred(p: BPred) extends SBPred {
    def apply(s: Sample, u: Bed) = p(u)
  }
    
  
  /** Lift function A => B to functions SObj[A] => SObj[B],
   *  BObj[A] => BObj[B], and SBObj[A] => SBObj[B]. In essence,
   *  an arbitrary function A => B is declared/injected into
   *  GMQL.
   */ 

  case class FunObj[A,B](f: A => B) {
    def apply(x: SObj[A]) = GenSObj((s: Sample) => f(x(s)))
    def apply(x: BObj[A]) = GenBObj((u: Bed) => f(x(u)))
    def apply(x: SBObj[A]) = GenSBObj((s: Sample, u: Bed) => f(x(s,u)))
  }

  /** An example of making sqrt a GMQL function.
   */

  val sqrt = FunObj(scala.math.sqrt _)



  //
  // Auxiliary structures for implicitly passing around arithmetics.
  // Needed these to get around the type system of Scala.
  //


  case class ArithBO[A](ev: (A, A) => A, y: BObj[A], x: BObj[A])
  extends BObj[A] {
    def apply(u: Bed) = ev(y(u), x(u))
    def chk(u: Bed, f: A => Boolean) = false
  }

  case class ArithSO[A](ev: (A, A) => A, y: SObj[A], x: SObj[A])
  extends SObj[A] {
    def apply(s: Sample) = ev(y(s), x(s))
    def chk(s: Sample, f: A => Boolean) = false
  }

  case class ArithSBO[A](ev: (A, A) => A, y: SBObj[A], x: SBObj[A])
  extends SBObj[A] {
    def apply(s: Sample, u: Bed) = ev(y(s, u), x(s, u))
    def chk(s: Sample, u: Bed, f: A => Boolean) = false
  }

  case class ArithSB[A](ev: (A, A) => A, y: SObj[A], x: BObj[A])
  extends SBObj[A] {
    def apply(s: Sample, u: Bed) = ev(y(s), x(u))
    def chk(s: Sample, u: Bed, f: A => Boolean) = false
  }

  case class ArithBS[A](ev: (A, A) => A, y: BObj[A], x: SObj[A])
  extends SBObj[A] {
    def apply(s: Sample, u: Bed) = ev(y(u), x(s))
    def chk(s: Sample, u: Bed, f: A => Boolean) = false
  }
  

}  // End object Predicates





