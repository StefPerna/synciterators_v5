

package synchrony
package gmql

//
// Wong Limsoon
// 4/4/2020
//


object Projections {

  import synchrony.genomeannot.BedWrapper._
  import synchrony.gmql.Samples._
  import synchrony.gmql.Predicates._
  import synchrony.iterators.AggrCollections.OpG._
  import synchrony.iterators.AggrCollections.implicits._
  import synchrony.programming.Sri
  import scala.language.implicitConversions

  case class ProjectionError(msg:String) extends Throwable

  var debug = true

  type Bed = SimpleBedEntry
  type Track = Iterator[Bed]
  type Aggr[A,B] = Sri.Sri[A,B]
  type AggrGen[A,B] = (A=>B)=>Aggr[A,B]
  type Meta = Map[String,Any]


  abstract class SProj {
    def apply(s:Sample):Meta
  }

  case class SProjSimple[A](k:String, v:SObj[A])
  extends SProj {
    def apply(s:Sample) = Map(k -> v(s))
  }

  case class SProjSBAggr[A](k:String, sa:SBAggr[A])
  extends SProj {
    def apply(s:Sample) = Map(k -> sa(s))
  }

  case class SProjGen[A](k:String, f:Sample=>A)
  extends SProj {
    def apply(s:Sample) = Map(k -> f(s))
  }

  case class SProjBAggr[A](k:String, a:BAggr[A])
  extends SProj {
    def apply(s:Sample) = Map(k -> a(s.track))
    def apply(vs:Vector[Bed]) = Map(k -> a(vs))
  }



  implicit class SPObj[A](k:String) extends SProj with BProj
  {
    // projections on sample
   
    def apply(s:Sample) = Map(k -> s.getM(k)) 

    def as(v:SObj[A])   = {
      println(if (debug) "**SProjSimple**" else "")
      SProjSimple(k, v)  
    }

    def as(v:Sample=>A) = {
      println(if (debug) "**SProjGen**" else "")
      SProjGen(k, v)  
    }

    def as(v:BAggr[A])  = { 
      println(if (debug) "**SProjBAggr**" else "")
      SProjBAggr(k, v)  
    }
                          
    def as(v:SBAggr[A]) = { 
      println(if (debug) "**SProjSBAggr**" else "")
      SProjSBAggr(k, v) 
    }

    // projections on region/track

    override def apply(u:Bed)         = Map(k -> u.getMisc(k)) 
    override def apply(s:Sample,u:Bed)= Map(k -> u.getMisc(k))

    def as(v:BObj[A]) = {
      println(if (debug) "**BProjSimple**" else "")
      BProjSimple(k, v)  
    }

    def as(v:Bed=>A) = {
      println(if (debug) "**BProjGen**" else "")
      BProjGen(k, v)  
    }

    def as(v:SBObj[A]) = {
      println(if (debug) "**SBProjSimple**" else "")
      SBProjSimple(k, v)  
    }

    def as(v:(Sample,Bed)=>A) = {
      println(if (debug) "**SBProjGen**" else "")
      SBProjGen(k, v)  
    }

    // projections on groupby aggregate functions

    def as(v: VSAggr[A]) = {
      println(if (debug) "**SGroupByProj**" else "")
      SGroupByProj(k, v)
    }

  }

  
  case class BAggr[A](a:Aggr[Bed,A]) 
  extends SObj[A] { 
    def apply(r:Track):A = r.flatAggregateBy(a)
    def apply(s:Sample) = this(s.track)
    def apply(rs:Vector[Bed]):A = rs.flatAggregateBy(a)
    def chk(s:Sample, f:A=>Boolean) = f(this(s))
  }


  case class SBAggr[A](sa:Sample=>Aggr[Bed,A])
  extends SObj[A] {
    def apply(s:Sample):A = s.track.flatAggregateBy(sa(s))
    def apply(s:Sample, rs:Vector[Bed]):A = rs.flatAggregateBy(sa(s))
    def chk(s:Sample, f:A=>Boolean) = f(this(s))
  }


  trait BProj {
    def apply(u:Bed):Meta
    def apply(s:Sample, u:Bed):Meta
  }

  case class BProjSimple[A](k:String, v:BObj[A])
  extends BProj {
    override def apply(u:Bed) = Map(k -> v(u))
    override def apply(s:Sample, u:Bed) = Map(k -> v(u))
  }

  case class BProjGen[A](k:String, f:Bed=>A)
  extends BProj {
    override def apply(u:Bed) = Map(k -> f(u))
    override def apply(s:Sample, u:Bed) = Map(k -> f(u))
  }

  case class SBProjSimple[A](k:String, v:SBObj[A])
  extends BProj {
    override def apply(u:Bed)=throw ProjectionError("SBProjSimple: Need sample")
    override def apply(s:Sample, u:Bed) = Map(k -> v(s, u))
  }

  case class SBProjGen[A](k:String, f:(Sample,Bed)=>A)
  extends BProj {
    override def apply(u:Bed)=throw ProjectionError("SBProjSimple: Need sample")
    override def apply(s:Sample, u:Bed) = Map(k -> f(s,u))
  }


  //
  // Groupby and aggregate functions on Vector[Sample]

  case class VSAggr[A](sa:Aggr[Sample,A]) {
    def apply(vs:Vector[Sample]):A = vs.iterator.flatAggregateBy(sa)
  }

  case class SGroupByProj[A](k:String, a:VSAggr[A]) {
    def apply(vs:Vector[Sample]) = Map(k -> a(vs))
  }

  //
  // Groupby and aggregate functions on Vector[Bed]
  
  type BGroupByProj[A] = SProjBAggr[A]

  // In GMQL groupby on regions, if no grouping key
  // is specified, locus is used. Simulate this by
  // providing locusOnly as default region grouping key.

  val locusOnly = MkStringBObj("")


  //
  // Lifting some commonly used aggregate functions
  // into BAggr, SBAggr, and VSAggr for use in
  // different contexts (apply on Samples,
  // on Tracks, etc.)

  trait FunAggrGen[A]
  { 
    def f[B]:AggrGen[B,A]

    def of(h:BObj[A]) = BAggr(f((u:Bed) => h(u)))
    def of(sh:SBObj[A]) = SBAggr((s:Sample) => f((u:Bed) => sh(s,u))) 
    def of(h:SObj[A]) = VSAggr(f((s:Sample) => h(s))) 
  }

  case object Average  extends FunAggrGen[Double]{override def f[B] = average}
  case object Sum      extends FunAggrGen[Double]{override def f[B] = sum}
  case object Biggest  extends FunAggrGen[Double]{override def f[B] = biggest}
  case object Smallest extends FunAggrGen[Double]{override def f[B] = smallest}


  trait BaseAggrGen[A]
  {
    def a[B]:Aggr[B,A]
  }

  case object Count extends BaseAggrGen[Double] {override def a[B] = count}
  implicit def Base2BAggr[A](x:BaseAggrGen[A]) = BAggr(x.a)
  implicit def Base2VSAggr[A](x:BaseAggrGen[A]) = VSAggr(x.a)



  // Here are some auxiliary functions to emulate
  // GMQL's more restricted/specialised join
  // predicates and output formats.
  //


  abstract class CanJoinS {
    def apply(x:Sample, y:Sample):Boolean
  }

  implicit class BaseCanJoinS(p:String) extends CanJoinS 
  { //
    // BaseCanJoinS emulates GMQL's rather restricted
    // join predicate on samples. 

    def apply(x:Sample, y:Sample):Boolean =
      x.checkM[Any](p, u => 
      y.checkM[Any](p, v => u == v))
  }

  implicit class GenCanJoinS(f:(Sample,Sample)=>Boolean)
  extends CanJoinS {
    def apply(x:Sample, y:Sample):Boolean = f(x,y)
  }


  abstract class CanJoinR {
    def apply(x:Bed, y:Bed):Boolean
  }

  implicit class BaseCanJoinR(p:String) extends CanJoinR
  { //
    // BaseCanJoinR emulates GMQL's rather restricted
    // join predicate on regions. 

    def apply(x:Bed, y:Bed):Boolean =
      x.checkMisc[Any](p, u => 
      y.checkMisc[Any](p, v => u == v))
  }

  implicit class GenCanJoinR(f:(Bed,Bed)=>Boolean)
  extends CanJoinR {
    def apply(x:Bed, y:Bed):Boolean = f(x,y)
  }

}



