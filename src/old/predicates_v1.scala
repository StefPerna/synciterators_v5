
package synchrony
package gmql

//
// Wong Limsoon
// 24/1/2020
//


object Predicates {

  import synchrony.genomeannot.GenomeAnnot._
  import synchrony.genomeannot.BedWrapper._
  import synchrony.gmql.Samples._
  import synchrony.gmql.Samples.SampleFile._
  import synchrony.programming.Arith._

  type Bed = SimpleBedEntry

  case class PredicateError(msg: String) extends Throwable


  def condSB(ps:SBPred*)(s:Sample, u:Bed) = ps.forall(p => p(s,u))
  def condB(ps:BPred*)(u:Bed) = ps.forall(p => p(u))
  def condSP(ps:SPred*)(s:Sample) = ps.forall(p => p(s))


  //
  // Predicates on a sample and a locus on its track
  //


  abstract class SBPred 
  { //
    // Placeholder for various predicates on
    // a sample *and* a locus on its track.

    def apply(s:Sample, u:Bed):Boolean
    def and(that:SBPred):SBPred = AndSB(this, that)
    def or(that:SBPred):SBPred = OrSB(this, that)
    def not:SBPred = NotSB(this)
  }

  case class AndSB(su:SBPred, sv:SBPred) extends SBPred {
    def apply(s:Sample, u:Bed) = su(s,u) && sv(s,u)
  } 
    
  case class OrSB(su:SBPred, sv:SBPred) extends SBPred {
    def apply(s:Sample, u:Bed) = su(s,u) || sv(s,u)
  } 
    
  case class NotSB(su:SBPred) extends SBPred {
    def apply(s:Sample, u:Bed) = !su(s,u)
  }

  case class GenSBPred(f:(Sample,Bed)=>Boolean) extends SBPred {
    def apply(s:Sample, u:Bed) = f(s,u)
  }

  implicit class MkSBPred(f:Sample=>BPred) extends SBPred {
    def apply(s:Sample, u:Bed) = f(s)(u)
  }



  //
  // Predicates on GenomeLocus/SimpleBedEntry
  //


  abstract class BPred
  { //
    // Placeholder for predicates on a *single* locus.

    def apply(u:Bed):Boolean
    def and(that:BPred):BPred = AndB(this, that)
    def or(that:BPred):BPred = OrB(this, that)
    def not:BPred = NotB(this)
  }
  
  case class NotB(u:BPred) extends BPred {
    def apply(x:Bed) = !u(x)
  }

  case class AndB(u:BPred, v:BPred) extends BPred {
    def apply(x:Bed) = u(x) && v(x)
  }
      
  case class OrB(u:BPred, v:BPred) extends BPred {
    def apply(x:Bed) = u(x) || v(x)
  }

  implicit class GenBPred(f:Bed=>Boolean) extends BPred {
    def apply(x:Bed) = f(x)
  }


  //
  // Predicates on Samples
  //


  abstract class SPred 
  { //
    // Placeholder for predicates on a Sample

    def apply(s:Sample):Boolean
    def and(that:SPred):SPred = AndS(this, that)
    def or(that:SPred):SPred = OrS(this, that)
    def not:SPred = NotS(this)
  }

  case class AndS(u:SPred, v:SPred) extends SPred {
    def apply(s:Sample) = u(s) && v(s)
  }

  case class OrS(u:SPred, v:SPred) extends SPred {
    def apply(s:Sample) = u(s) || v(s)
  }

  case class NotS(u:SPred) extends SPred {
    def apply(s:Sample) = !u(s)
  }
 
  implicit class GenSPred(f:Sample=>Boolean) extends SPred {
    def apply(s:Sample) = f(s)
  }
      
  //
  // semiJoin is needed for simulating GMQL.

  case class Incl(f:String*)
  case class Excl(f:String*)
  // case class SemiJoin(incl:Incl=Incl(), excl:Excl=Excl(), exDB:Vector[Sample])
  case class SemiJoin(incl:Incl=Incl(), excl:Excl=Excl(), exDB:SampleFile) 
  extends SPred {
    def apply(s:Sample) = exDB.iterator.exists(t =>
      incl.f.forall (l=>s.checkM[Any](l,u=>t.checkM[Any](l,v=>u==v))) &&
      excl.f.forall (l=>s.checkM[Any](l,u=>t.checkM[Any](l,v=>u!=v))))

    def chk(s:Sample) = false
  }



  //
  // Structures to turn various attributes of
  // a GenomeLocus into BObj, so that
  // predicates can be built from these.
  //


  abstract class BObj[A] 
  {
    // Provide these two functions to instantiate a BObj.
    // A BObj is an attribute of a GenomeLocus u. Thus,
    // this.apply(u) or this(u) should give the value of
    // this attribute. chk(u,f) should be equal to f(this(u)),
    // if this BObj is indeed an attribute of the 
    // GenomeLocus u; chk(u,f) is false otherwise. 

    def apply(u:Bed):A
    def chk(u:Bed, f:A=>Boolean):Boolean

    // Here are the BObj functions...

    type BO = BObj[A]
    type CMP = (A,A)=>Boolean
    def cmp(ev:CMP,m:BO) = GenBPred(u=> m.chk(u, x=> chk(u, y=> ev(y,x))))
 
    def ===(m:BO):BPred = GenBPred(u => m.chk(u, x=> chk(u, _ == x)))
    def !==(m:BO):BPred = GenBPred(u => m.chk(u, x=> chk(u, _ != x)))
    def >=(m:BO)(implicit ev:Ordering[A]):BPred = cmp(ev.gteq, m)
    def >(m:BO)(implicit ev:Ordering[A]):BPred = cmp(ev.gt, m)
    def <=(m:BO)(implicit ev:Ordering[A]):BPred = cmp(ev.lteq, m)
    def <(m:BO)(implicit ev:Ordering[A]):BPred = cmp(ev.lt, m)

    def +(m:BO)(implicit ev:Arith[A]):BO = ArithBO[A](ev.addA, this, m)
    def -(m:BO)(implicit ev:Arith[A]):BO = ArithBO[A](ev.subA, this, m)
    def *(m:BO)(implicit ev:Arith[A]):BO = ArithBO[A](ev.mulA, this, m)
    def /(m:BO)(implicit ev:Arith[A]):BO = ArithBO[A](ev.divA, this, m)
  }

  //
  // Turn the attributes of GenomeLocus into BObj,
  // so that predicates can be made from them.

  case object Chr extends BObj[String] {
    def apply(u:Bed) = u.chrom
    def chk(u:Bed, f:String=>Boolean) = f(this(u))
  }

  case object Start extends BObj[Int] {
    def apply(u:Bed) = u.chromStart
    def chk(u:Bed, f:Int=>Boolean) = f(this(u))
  } 

  case object End extends BObj[Int] {
    def apply(u:Bed) = u.chromEnd
    def chk(u:Bed, f:Int=>Boolean) = f(this(u))
  }

  // Turn the attributes from SimpleBedEntry
  // into BObj, so that predicates can
  // be made from them.

  case object Name extends BObj[String] {
    def apply(u:Bed) = u.name
    def chk(u:Bed, f:String=>Boolean) = f(this(u))
  }

  case object Strand extends BObj[String] {
    def apply(u:Bed) = u.strand
    def chk(u:Bed, f:String=>Boolean) = f(this(u))
  }
  
  case object Score extends BObj[Double] {
    def apply(u:Bed) = u.score
    def chk(u:Bed, f:Double=>Boolean) = f(this(u))
  }
  
  case class GenBObj[A](g:Bed=>A) extends BObj[A] {
    def apply(u:Bed) = g(u)
    def chk(u:Bed, f:A=>Boolean) = f(g(u))
  }

  //
  // SimpleBedEntry contains also meta data in a Map.
  // Turn these into BObj too.

  case class MetaR[A](k:String) extends BObj[A] {
    def apply(u:Bed) = u.getMisc[A](k)
    def chk(u:Bed, f:A=>Boolean) = u.checkMisc(k, f)
  }


  abstract class SObj[A] 
  { 
    // Provide these two functions to instantiate an SObj.
    // An SObj is an attribute of a Sample s. Thus, this(s)
    // or this.apply(s) should give the value of this
    // attribute. chk(s,f) should be equal to f(this(u)),
    // if this SObj is indeed an attribute of Sample s; 
    // chk(s,f) is false otherwise. 

    def apply(s:Sample):A
    def chk(s:Sample, f:A=>Boolean):Boolean

    // Here are the SObj functions...

    type SO = SObj[A]
    type CMP = (A,A)=>Boolean
    def cmp(ev:CMP,m:SO) = GenSPred(s=> m.chk(s, x=> chk(s, y=> ev(y,x))))

    def ===(m:SO):SPred = GenSPred(s => m.chk(s, x=> chk(s, _ == x)))
    def !==(m:SO):SPred = GenSPred(s => m.chk(s, x=> chk(s, _ != x)))
    def >=(m:SO)(implicit ev:Ordering[A]): SPred = cmp(ev.gteq, m)
    def >(m:SO)(implicit ev:Ordering[A]): SPred = cmp(ev.gt, m)
    def <=(m:SO)(implicit ev:Ordering[A]): SPred = cmp(ev.lteq, m)
    def <(m:SO)(implicit ev:Ordering[A]): SPred = cmp(ev.lt, m)

    def +(m:SO)(implicit ev:Arith[A]):SO = ArithSO[A](ev.addA, this, m)
    def -(m:SO)(implicit ev:Arith[A]):SO = ArithSO[A](ev.subA, this, m)
    def *(m:SO)(implicit ev:Arith[A]):SO = ArithSO[A](ev.mulA, this, m)
    def /(m:SO)(implicit ev:Arith[A]):SO = ArithSO[A](ev.divA, this, m)
  }

  case class GenSObj[A](g:Sample=>A) extends SObj[A] {
    def apply(s:Sample) = g(s)
    def chk(s:Sample, f:A=>Boolean) = f(g(s))
  }

  //
  // Sample contains meta data in a Map.
  // Turn these into SObj.

  case class MetaS[A](k:String) extends SObj[A] {
    def apply(s:Sample) = s.getM[A](k)
    def chk(s:Sample, f:A=>Boolean) = s.checkM(k, f)
  }


  abstract class SBObj[A]
  { 
    // Provide these two functions to instantiate an SBObj.

    def apply(s:Sample, u:Bed):A
    def chk(s:Sample, u:Bed, f:A=>Boolean):Boolean

    // Here are the SBObj functions...

    type SBO = SBObj[A]
    type CMP = (A,A)=>Boolean

    def cmp(ev:CMP, m:SBO) = 
      GenSBPred({case(s,u) => chk(s, u, y => m.chk(s, u, x => ev(y,x)))})
    
    def ===(m:SBO)=GenSBPred({case(s,u)=> chk(s,u,y=> m.chk(s,u,x=> y == x))})
    def !==(m:SBO)=GenSBPred({case(s,u)=> chk(s,u,y=> m.chk(s,u,x=> y != x))})
    def >=(m:SBO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gteq, m)
    def >(m:SBO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gt, m)
    def <=(m:SBO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lteq, m)
    def <(m:SBO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lt, m)

    def +(m:SBO)(implicit ev:Arith[A]):SBO = ArithSBO[A](ev.addA, this, m)
    def -(m:SBO)(implicit ev:Arith[A]):SBO = ArithSBO[A](ev.subA, this, m)
    def *(m:SBO)(implicit ev:Arith[A]):SBO = ArithSBO[A](ev.mulA, this, m)
    def /(m:SBO)(implicit ev:Arith[A]):SBO = ArithSBO[A](ev.divA, this, m)
  }

  case class GenSBObj[A](g:(Sample,Bed)=>A) extends SBObj[A] {
    def apply(s:Sample, u:Bed) = g(s, u)
    def chk(s:Sample, u:Bed, f:A=>Boolean) = f(g(s,u))
  }

  
  //
  // Conversions of String, Int, etc. into
  // SBObj, SObj and BObj. 
  //

   
  implicit class MkBObj[A<:AnyVal](val a:A) extends BObj[A] 
  {
    def apply(u:Bed) = a
    def chk(u:Bed, f:A=>Boolean) = f(a)
  }

  implicit class MkStringBObj(val a:String) extends BObj[String]
  {
    def apply(u:Bed) = a
    def chk(u:Bed, f:String=>Boolean) = f(a)
  }

  implicit class MkSObj[A<:AnyVal](val a:A) extends SObj[A] 
  {
    def apply(s:Sample) = a
    def chk(s:Sample, f:A=>Boolean) = f(a)
  }

  implicit class MkStringSObj(val a:String) extends SObj[String]
  {
    def apply(s:Sample) = a
    def chk(s:Sample, f:String=>Boolean) = f(a)
  }

  
  implicit class LiftS[A](m:SObj[A])
  {
    type BO = BObj[A]
    type SBO = SBObj[A]

    def cmp(ev:(A,A)=>Boolean, n:BO) =
      GenSBPred({case(s,u)=> m.chk(s, y => n.chk(u, x=> ev(y, x)))}) 

    def ===(n:BO):SBPred =
      GenSBPred({case(s,u)=> m.chk(s, y => n.chk(u, x=> y == x))})

    def !==(n:BO):SBPred =
      GenSBPred({case(s,u)=> m.chk(s, y => n.chk(u, x=> y != x))})

    def >=(n:BO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gteq, n)
    def >(n:BO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gt, n)
    def <=(n:BO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lteq, n)
    def <(n:BO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lt, n)
    
    def +(n:BO)(implicit ev:Arith[A]):SBO = ArithSB[A](ev.addA, m, n)
    def -(n:BO)(implicit ev:Arith[A]):SBO = ArithSB[A](ev.subA, m, n)
    def *(n:BO)(implicit ev:Arith[A]):SBO = ArithSB[A](ev.mulA, m, n)
    def /(n:BO)(implicit ev:Arith[A]):SBO = ArithSB[A](ev.divA, m, n)
  }


  implicit class LiftR[A](m:BObj[A]) 
  {
    type SO = SObj[A]
    type SBO = SBObj[A]

    def cmp(ev:(A,A)=>Boolean, n:SO) =
      GenSBPred({case(s,u)=> m.chk(u, y => n.chk(s, x=> ev(y, x)))}) 

    def ===(n:SO):SBPred =
      GenSBPred({case(s,u)=> m.chk(u, y => n.chk(s, x=> y == x))})

    def !==(n:SO):SBPred =
      GenSBPred({case(s,u)=> m.chk(u, y => n.chk(s, x=> y != x))})

    def >=(n:SO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gteq, n)
    def >(n:SO)(implicit ev:Ordering[A]): SBPred = cmp(ev.gt, n)
    def <=(n:SO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lteq, n)
    def <(n:SO)(implicit ev:Ordering[A]): SBPred = cmp(ev.lt, n)
    
    def +(n:SO)(implicit ev:Arith[A]):SBO = ArithBS[A](ev.addA, m, n)
    def -(n:SO)(implicit ev:Arith[A]):SBO = ArithBS[A](ev.subA, m, n)
    def *(n:SO)(implicit ev:Arith[A]):SBO = ArithBS[A](ev.mulA, m, n)
    def /(n:SO)(implicit ev:Arith[A]):SBO = ArithBS[A](ev.divA, m, n)
  }

  
  implicit class S2SBPred(p:SPred) extends SBPred
  {
    def apply(s:Sample, u:Bed) = p(s)
  }

  implicit class L2SBPred(p:BPred) extends SBPred
  {
    def apply(s:Sample, u:Bed) = p(u)
  }
    
  
  //
  // Coversion of functions to apply on SObj and BObj.
  //


  case class FunObj[A,B](f:A=>B) {
    def apply(x: SObj[A]) = GenSObj((s:Sample) => f(x(s)))
    def apply(x: BObj[A]) = GenBObj((u:Bed) => f(x(u)))
    def apply(x: SBObj[A]) = GenSBObj((s:Sample, u:Bed) => f(x(s,u)))
  }

  val sqrt = FunObj(scala.math.sqrt _)



  //
  // Auxiliary structures for implicitly
  // passing around arithmetics. Needed
  // these to get around the type system.


  case class ArithBO[A](ev:(A,A)=>A, y:BObj[A], x:BObj[A])
  extends BObj[A] {
    def apply(u:Bed) = ev(y(u), x(u))
    def chk(u:Bed, f:A=>Boolean) = false
  }

  case class ArithSO[A](ev:(A,A)=>A, y:SObj[A], x:SObj[A])
  extends SObj[A] {
    def apply(s:Sample) = ev(y(s), x(s))
    def chk(s:Sample, f:A=>Boolean) = false
  }

  case class ArithSBO[A](ev:(A,A)=>A, y:SBObj[A], x:SBObj[A])
  extends SBObj[A] {
    def apply(s:Sample, u:Bed) = ev(y(s,u), x(s,u))
    def chk(s:Sample, u:Bed, f:A=>Boolean) = false
  }

  case class ArithSB[A](ev:(A,A)=>A, y:SObj[A], x:BObj[A])
  extends SBObj[A] {
    def apply(s:Sample, u:Bed) = ev(y(s), x(u))
    def chk(s:Sample, u:Bed, f:A=>Boolean) = false
  }

  case class ArithBS[A](ev:(A,A)=>A, y:BObj[A], x:SObj[A])
  extends SBObj[A] {
    def apply(s:Sample, u:Bed) = ev(y(u), x(s))
    def chk(s:Sample, u:Bed, f:A=>Boolean) = false
  }
  

}





