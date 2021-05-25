

package synchrony.gmql

/** GenoMetric Query Language (GMQL)
 *
 *  Implement GMQL, a domain-specific language for querying Bed-like files.
 *  This module provides support for specifying projections on meta and
 *  region attributes.
 *
 *  The projections are:
 *
 *  SProj represents a projection on a Sample. It means selecting a subset of
 *  the meta attributes of that Sample. 
 *
 *  BProj represents a projection on a Bed entry. It means selecting a
 *  subset of the region attributes of that Bed entry. 
 *
 *
 * Wong Limsoon
 * 13 May 2020.
 */



object Projections {

  import scala.language.implicitConversions
  import synchrony.gmql.Samples
  import synchrony.gmql.Samples.implicits._
  import synchrony.gmql.Predicates._
  import synchrony.programming.Sri
  import synchrony.gmql.Samples.SampleFile.OpG.{
    average, sum, smallest, biggest, count
  }



  case class ProjectionError(msg:String) extends Throwable

  var DEBUG = true

  type Bed = Samples.Bed
  type Track = Samples.BedEIterator
  type Sample = Samples.Sample
  type Meta = Samples.Meta
  type Aggr[A,B] = Sri.Sri[A,B]
  type AggrGen[A,B] = (A => B) => Aggr[A,B]



  // The abstract syntax for projection is:
  //
  // SPObj ::= k:String as SProj
  //        |  k:String as BProj
  //
  // SProj ::= SObj[A]  |  SBAggr[A] |  BAggr[A] |  f: Sample => A
  //
  // BProj ::= BObj |  SBObj[A] |  VSAggr[A] 
  //        |  f:Bed => A  |  f:(Sample, Bed) => A 
  //
  // The abstract syntax for SObj and BObj is described in 
  // synchrony.gmql.Predicates, and is extended with 
  // aggregate functions here.
  //
  // SObj  ::= SObj aop SObj  |  MetaS[A](k: String)
  //        |  MkSObj[A<:AnyVal](a:A)  |  MkStringSObj[a:String]
  //        |  SBAggr 
  //        |  BAggr
  //
  // BObj   ::= Chr | Start | End | Strand | Score | Name | BObj aop Bobj 
  //         | GenBObj[A](f: Bed => A) | MetaR[A](k: String)
  //         | MkBObj[A<:AnyVal](a:A) | MkStringBObj[a:String]
  //
  // aop   ::= + | - | * | /
  //
  // SBAggr ::= Average of SBObj
  //         |  Sum of SBObj
  //         |  Biggest of SBObj
  //         |  Smallest of SBObj
  //         |  Count
  //
  // BAggr  ::= Average of BObj
  //         |  Sum of BObj
  //         |  Biggest of BObj
  //         |  Smallest of BObj
  //         |  Count
  //
  // VSAggr ::= Average of SObj
  //         |  Sum of SObj
  //         |  Biggest of SObj
  //         |  Smallest of SObj
  //         |  Count
  //



  /** SPObj implements the syntax rules
   *    SPObj ::= k:String as SProj
   *           |  k:String as BProj
   *
   *    SProj ::= SObj[A]                  -realised by SProjSimple[A].
   *           |  SBAggr[A]                -realised by SProjSBAggr[A].
   *           |  BAggr[A]                 -realised by SProjBAggr[A].
   *           |  f: Sample => A           -any function on Sample.
   *
   *    BProj ::= BObj                     -realised by BProjSimple[A]
   *           |  SBObj[A]                 -realised by SBProjSimple[A]
   *           |  f:Bed => A               -realised by any Bed => A
   *           |  f:(Sample, Bed) => A     -realised by any (Sample, Bed) => A
   *           |  VSAggr[A]                -realised by SGroupByProj[A]
   */

  implicit class SPObj[A](override val k:String) extends SProj(k) with BProj {

  
  /** Apply this projection on a Sample.
   *
   *  Typically, a projection means extracting a subset of the
   *  Sample's meta attributes. However, often, the values of
   *  the meta attributes can be manipulated during the process,
   *  and new meta attributes can also be created during the process.
   *
   *  @param s is the Sample.
   *  @return this projection of Sample s.
   */
   
    def apply(s: Sample) = Map(k -> s.getM(k)) 

   
  /** Realise the parsing rule, SProj ::= SObj[A].
   *  It constructs a simple projection, which is
   *  typically just looking up a meta attribute
   *  in a Sample.
   */

    def as(v: SObj[A])   = {
      println(if (DEBUG) "**SProjSimple**" else "")
      SProjSimple(k, v)  
    }


  /** Realize the parsing rule, SProj ::= f: Sample => A.
   *  It is basically a declaration that makes the function
   *  f a projection on Sample.
   */

    def as(v: Sample => A) = {
      println(if (DEBUG) "**SProjGen**" else "")
      SProjGen(k, v)  
    }


  /** Realize the parsing rule, SProj ::= BAggr[A].
   *  It constructs a projection that computes an aggregate
   *  function on the Bed entries of a Sample.
   */

    def as(v: BAggr[A])  = { 
      println(if (DEBUG) "**SProjBAggr**" else "")
      SProjBAggr(k, v)  
    }
                          

  /** Realize the parsing rule, SProj ::= SBAggr[A].
   *  It constructs a projection that computes a
   *  function on a Sample, where the function is
   *  expected to access the Bed entries of the Sample.
   */
 
    def as(v: SBAggr[A]) = { 
      println(if (DEBUG) "**SProjSBAggr**" else "")
      SProjSBAggr(k, v) 
    }



  //
  // Here is the BProj part of the parsing rules for SPObj.
  //

  
  /** Apply this projection on a Bed entry;
   *  i.e. this SPObj is being used as a BProj.
   *
   *  Typically, a BProj means extracting a subset of the
   *  Bed entry's region attributes. However, often, the values
   *  of the region attributes can be manipulated during the process,
   *  and new region attributes can also be created during the process.
   *
   *  @param u is a Bed entry.
   *  @param s is a sample.
   *  @return this projection of Bed entry u of Sample s.
   */
  
    override def apply(u: Bed)            = Map(k -> u.getMisc(k)) 
    override def apply(s: Sample, u: Bed) = Map(k -> u.getMisc(k))


  /** Realize the parsing rule,  BProj ::= BObj.
   *  It constructs a simple projection, which is
   *  typically just looking up a meta attribute
   *  in a Bed entry.
   */ 

    def as(v: BObj[A]) = {
      println(if (DEBUG) "**BProjSimple**" else "")
      BProjSimple(k, v)  
    }


  /** Realize the parsing rule, BProj ::= f: Bed => A.
   *  It is basically a declaration that makes the function
   *  f a projection on Bed entry.
   */

    def as(v: Bed => A) = {
      println(if (DEBUG) "**BProjGen**" else "")
      BProjGen(k, v)  
    }


  /** Realize the parsing rule,  BProj ::= SBObj.
   *  It constructs a simple projection, which is
   *  typically just looking up a meta attribute
   *  in a Bed entry of a Sample s.
   */ 

    def as(v: SBObj[A]) = {
      println(if (DEBUG) "**SBProjSimple**" else "")
      SBProjSimple(k, v)  
    }


  /** Realize the parsing rule, BProj ::= f: (Sample, Bed) => A.
   *  It is basically a declaration that makes the function
   *  f a projection on a Bed entry of a Sample.
   */

    def as(v: (Sample, Bed) => A) = {
      println(if (DEBUG) "**SBProjGen**" else "")
      SBProjGen(k, v)  
    }


  /** Realize the parsing rule, BProj ::= VSAggr.
   *  It is a projection that computes an aggregate function
   *  on the Bed entries of a Sample, and keeps the result
   *  in a meta attribute of the Sample.
   */

    def as(v: VSAggr[A]) = {
      println(if (DEBUG) "**SGroupByProj**" else "")
      SGroupByProj(k, v)
    }

  }  // End class SPObj



  /** Realizing SProj ::= SObj | SBAggr | BAggr | f:Sample=> A 
   */

  abstract class SProj(k: String) {
    def apply(s: Sample): Meta
    def toPair = (k, (s: Sample) => apply(s)(k))
    def toKV = k -> ((s: Sample) => apply(s)(k))
  }


  case class SProjSimple[A](k: String, v: SObj[A])
  extends SProj(k) {
    def apply(s: Sample): Meta = Map(k -> v(s))
  }


  case class SProjSBAggr[A](k: String, sa: SBAggr[A])
  extends SProj(k) {
    def apply(s: Sample): Meta = Map(k -> sa(s))
  }


  case class SProjGen[A](k: String, f: Sample => A)
  extends SProj(k) {
    def apply(s: Sample): Meta = Map(k -> f(s))
  }


  case class SProjBAggr[A](k: String, a: BAggr[A])
  extends SProj(k) {
    def apply(s: Sample): Meta = Map(k -> a(s.track))
    def apply(vs: Vector[Bed]): Meta = Map(k -> a(vs))
  }



  /** Realizing BProj ::= BObj |  SBObj[A] | VSAggr[A]
   *                   |  f:Bed => A  |  f:(Sample, Bed) => A
   */
  
  trait BProj {
    val k: String
    def apply(u: Bed): Meta
    def apply(s: Sample, u: Bed): Meta
    def toUPair = (k, (u: Bed) => apply(u)(k))
    def toUKV = k -> ((u: Bed) => apply(u)(k))
    def toSUPair = (k, (s: Sample, u: Bed) => apply(s, u)(k))
    def toSUKV = k -> ((s: Sample, u: Bed) => apply(s, u)(k))
  }


  case class BProjSimple[A](override val k: String, v: BObj[A])
  extends BProj {
    override def apply(u: Bed) = Map(k -> v(u))
    override def apply(s: Sample, u: Bed) = Map(k -> v(u))
  }

  case class SBProjSimple[A](override val k: String, v: SBObj[A])
  extends BProj {
    override def apply(u: Bed) =
      throw ProjectionError("SBProjSimple: Need sample")

    override def apply(s: Sample, u: Bed) = Map(k -> v(s, u))
  }


  case class BProjGen[A](override val k: String, f: Bed => A)
  extends BProj {
    override def apply(u: Bed) = Map(k -> f(u))
    override def apply(s: Sample, u: Bed) = Map(k -> f(u))
  }


  case class SBProjGen[A](override val k: String, f: (Sample, Bed) => A)
  extends BProj {
    override def apply(u: Bed) =
      throw ProjectionError("SBProjSimple: Need sample")

    override def apply(s: Sample, u: Bed) = Map(k -> f(s, u))
  }



  /** Realize the parsing rules for aggregate functions,
   */

  case class BAggr[A](a: Aggr[Bed,A]) 
  extends SObj[A] { 
    def apply(r: Track): A = r.flatAggregateBy(a)
    def apply(s: Sample) = this(s.track)
    def apply(rs: Vector[Bed]):A = rs.flatAggregateBy(a)
    def chk(s: Sample, f: A => Boolean) = f(this(s))
  }


  case class SBAggr[A](sa: Sample => Aggr[Bed,A])
  extends SObj[A] {
    def apply(s: Sample): A = s.track.flatAggregateBy(sa(s))
    def apply(s: Sample, rs: Vector[Bed]):A = rs.flatAggregateBy(sa(s))
    def chk(s: Sample, f: A => Boolean) = f(this(s))
  }


  case class VSAggr[A](sa: Aggr[Sample, A]) {
    def apply(vs: Vector[Sample]): A = vs.iterator.flatAggregateBy(sa)
  }


  case class SGroupByProj[A](k: String, a: VSAggr[A]) {
    def apply(vs: Vector[Sample]) = Map(k -> a(vs))
  }



  // Groupby and aggregate functions on Vector[Bed]
  
  type BGroupByProj[A] = SProjBAggr[A]

  // In GMQL groupby on regions, if no grouping key
  // is specified, locus is used. Simulate this by
  // providing locusOnly as default region grouping key.

  val locusOnly = MkStringBObj("")



  /**
   * Lifting some commonly used aggregate functions into 
   * BAggr, SBAggr, and VSAggr for use in different contexts,
   * viz. apply on Samples, on Tracks, etc.
   */

  trait FunAggrGen[A] { 
    def f[B]: AggrGen[B,A]
    def of(h: BObj[A]) = BAggr(f((u: Bed) => h(u)))
    def of(sh: SBObj[A]) = SBAggr((s: Sample) => f((u: Bed) => sh(s, u))) 
    def of(h: SObj[A]) = VSAggr(f((s: Sample) => h(s))) 
  }

  case object Average  extends FunAggrGen[Double]{ override def f[B] = average }
  case object Sum      extends FunAggrGen[Double]{ override def f[B] = sum }
  case object Biggest  extends FunAggrGen[Double]{ override def f[B] = biggest }
  case object Smallest extends FunAggrGen[Double]{override def f[B] = smallest }


  case class GenAggrOverRegions[A](f: AggrGen[Bed, A]) {
    def of(h: BObj[A]) = BAggr(f((u: Bed) => h(u)))
    def of(sh: SBObj[A]) = SBAggr((s: Sample) => f((u: Bed) => sh(s, u))) 
  }

  case class GenAggrOverSamples[A](f: AggrGen[Sample, A]) {
    def of(h: SObj[A]) = VSAggr(f((s: Sample) => h(s))) 
  }
  


  trait BaseAggrGen[A] {
    def a[B]: Aggr[B,A]
  }

  case object Count extends BaseAggrGen[Double] { override def a[B] = count }

  def BaseAggrOverRegions[A](a: Aggr[Bed, A]) = BAggr[A](a)

  def BaseAggrOverSamples[A](a: Aggr[Sample, A]) = VSAggr[A](a)

  implicit def Base2BAggr[A](x: BaseAggrGen[A]) = BAggr(x.a)

  implicit def Base2VSAggr[A](x: BaseAggrGen[A]) = VSAggr(x.a)



  /**
   * Here are some auxiliary functions to emulate GMQL's more 
   * restricted/specialised join predicates and output formats.
   */


  abstract class CanJoinS {
    def apply(x: Sample, y: Sample): Boolean
  }


  implicit class BaseCanJoinS(p: String) extends CanJoinS { 

    // BaseCanJoinS emulates GMQL's rather restricted
    // join predicate on samples. 

    def apply(x: Sample, y: Sample): Boolean =
      x.checkM[Any](p, u => 
      y.checkM[Any](p, v => u == v))
  }


  implicit class GenCanJoinS(f: (Sample, Sample) => Boolean)
  extends CanJoinS {
    def apply(x: Sample, y: Sample): Boolean = f(x,y)
  }


  abstract class CanJoinR {
    def apply(x: Bed, y: Bed): Boolean
  }


  implicit class BaseCanJoinR(p: String) extends CanJoinR { 

    // BaseCanJoinR emulates GMQL's rather restricted
    // join predicate on regions. 

    def apply(x: Bed, y: Bed): Boolean =
      x.checkMisc[Any](p, u => 
      y.checkMisc[Any](p, v => u == v))
  }


  implicit class GenCanJoinR(f: (Bed, Bed) => Boolean)
  extends CanJoinR {
    def apply(x: Bed, y: Bed): Boolean = f(x, y)
  }

} // End object Projections





