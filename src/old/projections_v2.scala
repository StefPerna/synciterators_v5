

package synchrony
package gmql

//
// Wong Limsoon
// 27/1/2020
//


object Projections {

  import synchrony.genomeannot.BedWrapper._
  import synchrony.gmql.Samples._
  import synchrony.gmql.Predicates._
  import synchrony.iterators.AggrCollections.OpG._
  import synchrony.iterators.AggrCollections.implicits._
  import synchrony.programming.Sri

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
    def apply(s:Sample) = Map(k -> a(s.track()))
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

  }

  
  case class BAggr[A](a:Aggr[Bed,A]) extends SObj[A] { 
    def apply(r:Track):A = r.flatAggregateBy(a)
    def apply(s:Sample) = this(s.track())
    def chk(s:Sample, f:A=>Boolean) = f(this(s))
  }


  case class SBAggr[A](sa:Sample=>Aggr[Bed,A]) extends SObj[A] {
    def apply(s:Sample):A = s.track().flatAggregateBy(sa(s))
    def chk(s:Sample, f:A=>Boolean) = f(this(s))
  }


  abstract class BAggrGen[A](f:AggrGen[Bed,A]) {
    def of(h:BObj[A]) = BAggr(f((u:Bed) => h(u)))
    def of(sh:SBObj[A]) = SBAggr((s:Sample) => f((u:Bed) => sh(s,u))) 
  }

  case object Average extends BAggrGen[Double](average)
  case object Sum extends BAggrGen[Double](sum)
  case object Biggest extends BAggrGen[Double](biggest)
  case object Smallest extends BAggrGen[Double](smallest)
  val Count = BAggr[Double](count)



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
    override def apply(u:Bed) = throw ProjectionError("SBProjSimple: Need sample")
    override def apply(s:Sample, u:Bed) = Map(k -> v(s, u))
  }

  case class SBProjGen[A](k:String, f:(Sample,Bed)=>A)
  extends BProj {
    override def apply(u:Bed) = throw ProjectionError("SBProjSimple: Need sample")
    override def apply(s:Sample, u:Bed) = Map(k -> f(s,u))
  }


}



