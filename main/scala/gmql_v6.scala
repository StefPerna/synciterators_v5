

package synchrony
package gmql


//
// Wong Limsoon
// 25/1/2020
//
// This module is a simple emulation of Polimi's
// GMQL.  It shows how a genomic query language
// can be implemented in Synchrony.
//


object GMQL {

  import synchrony.gmql.Predicates._
  import synchrony.gmql.Projections._
  import synchrony.gmql.Samples._

  import synchrony.iterators.MiscCollections._
  import synchrony.iterators.AggrCollections._
  import synchrony.iterators.AggrCollections.OpG._
  import synchrony.iterators.AggrCollections.implicits._
  import synchrony.iterators.SyncCollections._
  import synchrony.iterators.SyncCollections.implicits._
  import synchrony.genomeannot.GenomeAnnot._
  import synchrony.genomeannot.GenomeAnnot.GenomeLocus._
  import synchrony.genomeannot.BedWrapper._
  import synchrony.programming.StepContainer._
  import synchrony.programming.Sri._
  

  type Bed = SimpleBedEntry
  type Track = Iterator[Bed]
  type Meta = Map[String,Any]
  

  case class DB(samples:Vector[Sample])
  {
    type SPred = DB.SPred
    type SBPred = DB.SBPred
    type SProj = DB.SProj

    type RProj = DB.RProj
    type SAggr = DB.SAggr
    type RAggr = DB.RAggr
    type canJoinS = DB.canJoinS
    type canJoinR = DB.canJoinR
    type OnSample[A] = DB.OnSample[A]
    type OnRegion[A] = DB.OnRegion[A]
    type BySample[G] = DB.BySample[G]
    type ByRegion[G] = DB.ByRegion[G]
    type Genometric = DB.Genometric


    def select(
      sample:OnSample[SPred]=DB.OnSample(),
      region:OnRegion[SBPred]=DB.OnRegion()) = DB.select(sample,region)(this)

    def extend(sample:OnSample[SProj]) = DB.extend(sample)(this)

    def project(
      sample:OnSample[SProj]=DB.OnSample(),
      region:OnRegion[BProj]=DB.OnRegion()) = DB.project(sample,region)(this)


/*
    def flatGroupBy[G,R](
      sample:BySample[G],
      region:ByRegion[R]) = DB.flatGroupBy(sample,region)(this)
    def flatGroupByS[G](g:Sample=>G)(a:SAggr*)=DB.flatGroupByS(g)(a:_*)(this)
    def flatGroupByR[G](g:Bed=>G)(a:RAggr*) = DB.flatGroupByR(g)(a:_*)(this)

    def groupbyS[G](g:Sample=>G)(a:SAggr*) = DB.groupbyS(g)(a:_*)(this)
    def groupbyR[G](g:Bed=>G)(a:RAggr*) = DB.groupbyR(g)(a:_*)(this)
    def partitionbyR[G](g:Bed=>G)(a:RAggr*) = DB.partitionbyR(g)(a:_*)(this)

    def map(
      region:OnRegion[RAggr], 
      joinbyS:canJoinS = {case (x,y)=>true})(edb:DB) = 
    {
      DB.map(region,joinbyS)(this,edb)
    }

    def difference(
      exact:Boolean = false,
      joinbyS:canJoinS = {case (x,y)=>true})(edb:DB)=
    {
      DB.difference(exact,joinbyS)(this,edb)
    }

    def join(
      limit:Int=1000,
      pred:Genometric = DB.Genometric(), 
      joinbyS:canJoinS = {case (x,y) =>true},
      joinbyR:canJoinR = {case (x,y) =>true},
      outputS:DB.OutputS=DB.OverwriteS(),
      outputR:DB.OutputR=DB.LeftR,
      orderR:DB.OrderR=DB.DoneR)(edb:DB) =
    {
      DB.join(limit,pred,joinbyS,joinbyR,outputS,outputR,orderR)(this,edb)
     }
*/

  }


  object DB 
  {
    type SPred = Predicates.SPred
    type SBPred = Predicates.SBPred
    type SProj = Projections.SProj
    type BProj = Projections.BProj
    type Aggr[A,B] = Projections.Aggr[A,B]

//    type SProj = Sample=>Sample
    type RProj = Bed=>Bed
    type SAggr = (String, Sri[Sample,Double]) 
    type RAggr = (String, Sri[Bed,Double])
    type canJoinS = (Sample,Sample)=>Boolean
    type canJoinR = (Bed,Bed)=>Boolean

    case class OnSample[A](p:A*)
    case class OnRegion[A](p:A*)
    case class BySample[G](grp:Sample=>G, aggr:SAggr*)
    case class ByRegion[G](grp:Bed=>G, aggr:RAggr*)
    case class Genometric(g:LocusPred*)


    //
    // Emulation of GMQL's select statement.
    //


    def select(
      sample:OnSample[SPred]  = OnSample(),
      region:OnRegion[SBPred] = OnRegion())(db:DB) =
    {
      Step0{       selectS(sample.p:_*)(db) }.
      Step1{ db => selectR(region.p:_*)(db) }.
      Done
    }

    private def selectS(f:SPred*)(db:DB) = 
      if (f.to(Vector).isEmpty) db 
      else  DB(for(s <- db.samples; if f.forall(_(s))) yield s) 

    private def selectR(f:SBPred*)(db:DB) =
      if (f.to(Vector).isEmpty) db
      else DB(for(s <- db.samples)
              yield s.updateT {for(r <- s.track();
                                   if f.forall(_(s,r)))
                               yield r})



    //
    // Emulation of GMQL's extend
    //


    private def split(
      ps:Vector[SProj],
      x:Vector[SProj],
      y:Vector[SProj]) : (Vector[SProj], Vector[SProj]) =
    {
      if (ps.isEmpty) (x, y) else ps.head match {
        case p:SPObj[_]       => split(ps.tail, x :+ p, y)
        case p:SProjSimple[_] => split(ps.tail, x :+ p, y)
        case p:SProjGen[_]    => split(ps.tail, x :+ p, y)
        case p:SProjBAggr[_]  => split(ps.tail, x, y :+ p)
        case p:SProjSBAggr[_] => split(ps.tail, x, y :+ p)
      }
    }

    private def mkAggr(ps:Vector[SProj], s:Sample) =
    {
      def cvt[A](a:Aggr[Bed,A]) = a.asInstanceOf[Aggr[Bed,Double]]
      val b=ps.map({
              case SProjSBAggr(k,sa)=> cvt(sa.sa(s)) |> ((x:Double)=>(k,x))
              case SProjBAggr(k,a)  => cvt(a.a) |> ((x:Double)=>(k,x))})
      def mix(b:Array[(String,Double)]) = Map(b.toIndexedSeq:_*)
      combineN(mix, b:_*)
    }


    def extend(sample:OnSample[SProj])(db:DB) =
    {
      val (sproj, sprojAggr) = split(sample.p.to(Vector), Vector(), Vector())

      Step0{       if (sproj.isEmpty) db else
                   DB(for(s <- db.samples)
                      yield { 
                        var sample = s
                        val ps = sproj.iterator
                        while (ps.hasNext) { 
                          sample = sample.mergeM(ps.next()(s))
                        }
                        sample })  }.

      Step1{ db => if (sprojAggr.isEmpty) db else
                   DB(for(s<-db.samples)
                   yield {
                     val aggr = mkAggr(sprojAggr, s)
                     s mergeM (s.track().flatAggregateBy(aggr))}) }.

      Done
    }



    //
    // Emulation of GMQL's project.
    //


    def project(
      sample:OnSample[SProj]=OnSample(),
      region:OnRegion[BProj]=OnRegion())(db:DB) =
    {
      Step0{       projectS(sample.p:_*)(db) }.
      Step1{ db => projectR(region.p:_*)(db) }.
      Done
    }

    private def projectS(p:SProj*)(db:DB) =
    {
      val (sproj, sprojAggr) = split(p.to(Vector), Vector(), Vector())

      Step0{       if (sproj.isEmpty) db else
                   DB(for(s <- db.samples)
                      yield { 
                        var sample = s.eraseM()
                        val ps = sproj.iterator
                        while (ps.hasNext) { 
                          sample = sample.mergeM(ps.next()(s))
                        }
                        sample })  }.

      Step1{ db => if (sprojAggr.isEmpty) db else
                   DB(for(s<-db.samples)
                   yield {
                     val aggr = mkAggr(sprojAggr, s)
                     s mergeM (s.track().flatAggregateBy(aggr))}) }.

      Done
    }


    private def projectR(p:BProj*)(db:DB) =
    {
      for(s <- db.samples)
      yield s.updateT {for(r <- s.track())
                       yield { var bed = r.eraseMisc()
                               val ps = p.iterator
                               while (ps.hasNext) {
                                 bed = bed mergeMisc (ps.next()(s,r))
                               }
                               bed }}
    }


/*

    //
    // Emulation of GMQL's groupby.
    //


    def flatGroupBy[G,R](sample:BySample[G], region:ByRegion[R])(db:DB) =
    {
      Step0{       flatGroupByS(sample.grp)(sample.aggr:_*)(db) }.
      Step1{ db => flatGroupByR(region.grp)(region.aggr:_*)(db) }.
      Done
    }

    def flatGroupByS[G](grp:Sample=>G)(a:SAggr*)(db:DB) =
    { //
      // This is GMQL's groupby. It is quite restrictive
      // due to its "flat" relational perspective.

      val bs = a.toVector.map({case (f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      if (a.to(Vector).isEmpty) 
        DB { (for ((g,acc) <- db.samples.groupby(grp)(keep); s <- acc)
              yield s mergeM (Map("group" -> g))).toVector }
      else 
        DB { (for ((g,(c,acc)) <- db.samples.groupby(grp)(withAcc(b)); s <- acc)
              yield s mergeM (Map("group" -> g) ++ c)).toVector }
    }

    def flatGroupByR[G](grp:Bed=>G)(a:RAggr*)(db:DB) =
    { //
      // This is GMQL's groupby aggregate function.
      // It is very restrictive. It forces locus to
      // be part of the grouping key. So only Bed entries
      // having the same loci can be in the same group.

      val c = ("count", count[Bed]) +: a.toVector
      val bs = c.map({case (f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      DB(for(s <- db.samples)
         yield s updateT {
                   (for((k, v) <- s.track().groupby(x => (x.locus, grp(x)))(b))
                    yield SimpleBedEntry(
                            k._1.chrom, 
                            k._1.chromStart,
                            k._1.chromEnd) .
                          addMisc("group", k._2) .
                          mergeMisc(v)).iterator }) 
    }


    // I prefer groupby that retains a naturally
    // nested grouping structure. So here it is...

    def groupbyS[G](grp:Sample=>G)(a:SAggr*)(db:DB) =
    { //
      // This is Synchrony's general groupby. It follows
      // the natural nested structure of grouping.

      val bs = a.to(Vector).map({case(f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      if (a.to(Vector).isEmpty)
        db.samples.groupby(grp)(keep)
      else
        db.samples.groupby(grp)(b)
    }


    def groupbyR[G](grp:Bed=>G)(a:RAggr*)(db:DB) =
    { //
      // This is Synchrony's generalized groupby on region/track.
      // It follows the natural nested structure of groups.
      // A user can later process each resulting group in
      // whatever ways he like, w/o forcing the groups into
      // Bed/track right now.

      val bs = a.to(Vector).map({case(f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*) 

      for(s <- db.samples) 
      yield if (a.to(Vector).isEmpty) (s, s.track().groupby(grp)(keep))
            else (s, s.track().groupby(grp)(b))
    }

    def partitionbyR[G](grp:Bed=>G)(a:RAggr*)(db:DB) =
    { //
      // Similar to Rgroupby, but assumes that the entries in
      // each group are sorted in a way consistent with "isBefore".

      val bs = a.to(Vector).map({case(f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      for(s <- db.samples) 
      yield if (a.to(Vector).isEmpty) (s, s.track().partitionby(grp)(keep))
            else (s, s.track().partitionby(grp)(b))
    }


    //
    // Emulation of GMQL's map.
    //


    def map(
      region:OnRegion[RAggr], 
      joinby:canJoinS = {case (x,y)=>true})(rdb:DB, edb:DB) =
    { //
      // GMQL has an unnecessarily restricted join predicate,
      // basically canJoinS(p1, p2, ...) where p1, p2, ...
      // are meta attributes, meaning two samples can join
      // if they agree on these meta attributes.
      //
      // In Synchrony, we allow more general join predicates,
      // viz. any Boolean function on a pair of samples.

      Step0{       mapS(joinby)(rdb, edb) }.
      Step1{ db => mapR(region.p:_*)(db) }.
      Done
    }

    def mapS(canJoin:canJoinS = {case (x,y)=>true})(rdb:DB, edb:DB) =
      (for(ref<-rdb.samples;
           e<-edb.samples;
           if (canJoin(ref,e)))
       yield (ref,e)) .
      groupby(_._1)(OpG.map(_._2)) .toVector

    def mapR(a:RAggr*)(db:Vector[(Sample,Vector[Sample])]) =
    {
      val as = ("count", count[Bed]) +: a.toVector
      val bs = as.map({case (f,aggr)=>compose(aggr,(x:Double)=>(f,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      for((ref,matches) <- db) yield
      {
        val lm = LmTrack(ref.track())
        val es =  for(e <- matches) yield Connectors.join(lm, e.track())  
        
        (ref, 
         for(r<-lm) yield (r,for(e<-es) yield (e syncWith r).aggregateBy(b)))
      }
    }


    //
    // Emulaton of GMQL's difference.
    //


    def difference(
      exact:Boolean = false,
      joinby:canJoinS = {case (x,y) => true})(rdb:DB, edb:DB) =
    {
      Step0{       mapS(joinby)(rdb,edb) }.
      Step1{ db => differenceR(exact)(db) }.
      Done
    }

    def differenceR(exact:Boolean =false)(db:Vector[(Sample,Vector[Sample])]) =
    {
      def overlap(x:Bed,y:Bed) = 
        if (exact) {x sameLocus y} 
        else x.overlap(1)(y)

      for((ref, diffs) <- db;
          lm = LmTrack(ref.track());
          ds = for(d <- diffs) yield Connectors.join(lm, d.track());
          tr = for(r <- lm; 
                   if forall((d:SyncedTrack[Bed,Bed]) => 
                        forall((x:Bed) => !overlap(r,x))(d syncWith r))(ds))
               yield r;
          if (tr.hasNext))
      yield ref updateT tr 
    }


    //
    // Emulation of GMQL's join
    //


    def join(
      limit:Int=1000,
      pred:Genometric=Genometric(), 
      joinbyS:canJoinS = {case (x,y) =>true},
      joinbyR:canJoinR = {case (x,y) =>true},
      outputS:OutputS = OverwriteS(),
      outputR:OutputR = LeftR,
      orderR:OrderR = DoneR)(rdb:DB, edb:DB) =
    {
      Step0{       mapS(joinbyS)(rdb, edb) }.
      Step1{ db => joinR(limit, pred, joinbyR, outputS, outputR, orderR)(db) }.
      Done
    }
      

    def joinR(
      limit:Int = 1000,
      pred:Genometric = Genometric(), 
      joinbyR:canJoinR = {case (x,y) =>true},
      outputS:OutputS = OverwriteS(),
      outputR:OutputR = LeftR,
      orderR:OrderR = DoneR)(db:Vector[(Sample,Vector[Sample])]) =
    { //
      // NOTE: GMQL automatically renames keys of meta data
      // before samples are merged/joined. Here,  although
      // I can, I dont automatically rename; users can invoke
      // renameM() on samples explicitly to do the renaming
      // themselves.
      //
      // NOTE: I also require a limit to be specified. This
      // limit translates to inserting an extra Genometric
      // predicate, DLE(limit), into the join. I.e., the 
      // regions to be joined should be within limit.
      //
      // NOTE: GMQL's genometric predicates include MD(k).
      // However, MD(k) is quite different from other
      // genometric predicates. Other genometric predicates
      // can be determined solely by looking at the pair
      // of loci being compared. In contast, MD(k) requires
      // one to check other loci besides the pair of loci
      // being compared. For Synchrony, we exclude MD(k)
      // as a genometric predicate. MD(k) can be expressed
      // in Synchrony via sliding-windows instead.
      //
      // NOTE: GMQL puts unnecessary restricted on join
      // predicate for regions, basically 
      // canJoinR(p1, p2, ...) where p1, p2, ... are meta
      // attributes of regions. I.e., two regions can
      // join if they agree on these attributes. In Synchrony,
      // we allow more general join predicates, viz. any
      // Boolean function on a pair of regions.

      for((anchor, expts) <- db;
          e <- expts;
          lm = LmTrack(anchor.track());
          ex = Connectors.join(
                 lmtrack=lm,
                 extrack=e.track(),
                 canSee=GenomeLocus.cond(GenomeLocus.DLE(limit)) _);
          tr = for(r <- lm;
                   t <- ex syncWith r;
                   if (joinbyR(r, t));
                   if (GenomeLocus.cond(pred.g:_*)(t,r));
                   joined <- outputR(r,t))
               yield joined)
      yield outputS(anchor, e).updateT(orderR(tr))
    }


    //
    // Here are some auxiliary functions to emulate
    // GMQL's more restricted/specialised join
    // predicates and output formats.
    //

    def canJoinS(p:String*)(x:Sample, y:Sample) =
    { //
      // canJoinS emulates GMQL's rather restricted
      // join predicate on samples. 

      p.forall(f => x.checkM[Any](f, u => 
                    y.checkM[Any](f, v => u == v)))
    }
    

    def canJoinR(p:String*)(x:Bed, y:Bed) =
    { //
      // canJoinR emulates GMQL's rather restricted
      // join predicate on regions.

      p.forall(f => x.checkMisc[Any](f, u => 
                    y.checkMisc[Any](f, v => u == v)))
    }

    
    abstract class OutputS {
      def apply(x:Sample, y:Sample): Sample
    }

    case object LeftS extends OutputS {
      def apply(x:Sample, y:Sample) = x
    }

    case object RightS extends OutputS {
      def apply(x:Sample, y:Sample) = y
    }

    case class OverwriteS(f:String=>String = (p => "r."++p))
    extends OutputS {
      def apply(x:Sample, y:Sample) = x overwriteM y.renameM(f).meta
    }
 
    case class GenS(f:(Sample,Sample)=>Sample) extends OutputS {
      def apply(x:Sample, y:Sample) = f(x,y)
    }
    

    abstract class OutputR {
      def apply(x:Bed, y:Bed): Option[Bed]
    }

    case object LeftR extends OutputR {
      def apply(x:Bed, y:Bed) = Some(x)
    }

    case object RightR extends OutputR {
      def apply(x:Bed, y:Bed) = Some(y)
    }

    case class IntR(f:String=>String = (p => "r."++p))
    extends OutputR {
      def apply(x:Bed, y:Bed) = (x.locus intersect y.locus) match {
        case None => None
        case Some(locus) => Some(SimpleBedEntry(locus).
                                 overwriteMisc(x.misc).
                                 overwriteMisc(y.renameMisc(f).misc))}
    }

    case class BothR(f:String=>String = (p => "r."++p))
    extends OutputR {
      def apply(x:Bed, y:Bed) = Some(x.overwriteMisc(Map(
                                 f("chrom") -> y.chrom,
                                 f("chromStart") -> y.chromStart,
                                 f("chromEnd") -> y.chromEnd)))
    }

    case class CatR(f:String=>String = (p => "r."++p))
    extends OutputR {
      def apply(x:Bed, y:Bed) = (x.locus union y.locus) match {
        case None => None
        case Some(locus) => Some(SimpleBedEntry(locus). 
                                 overwriteMisc(x.misc).
                                 overwriteMisc(y.renameMisc(f).misc))}
    }

    case class GenR(f:(Bed,Bed)=>Option[Bed]) extends OutputR {
      def apply(x:Bed, y:Bed) = f(x,y)
    }

   
    abstract class OrderR {
      def apply(it:Iterator[Bed]): Iterator[Bed]
    }

    case object DoneR extends OrderR {
      def apply(it:Iterator[Bed]) = it
    }

    case object DistinctR extends OrderR {
      def apply(it:Iterator[Bed]) = {
        var tmp: Option[Bed] = None
        for(b <- it;
            if (tmp match {
              case None => { tmp = Some(b); true}
              case Some(x) => if (b sameLocus x) false
                              else { tmp = Some(b); true }}))
        yield b
      }
    }

    case object SortR extends OrderR {
      def apply(it:Iterator[Bed]) = it.toVector.sorted.iterator
    }

    case object SortDistinctR extends OrderR {
      def apply(it:Iterator[Bed]) = DistinctR(SortR(it))
    }
*/
  }



  object Connectors
  { //
    // Functions for synchronizing ref and expt tracks
    //

    def connect(
      lmtrack:Iterator[Bed],
      extrack:Iterator[Bed],
      isBefore:(Bed,Bed)=>Boolean = GenomeLocus.isBefore _,
      canSee:(Bed,Bed)=>Boolean = GenomeLocus.cond(GenomeLocus.Overlap(1)) _,
      exclusiveCanSee: Boolean = false)
    : (LmTrack[Bed],SyncedTrack[Bed,Bed]) =
    {
      val itB = LmTrack(lmtrack)
      val itA = itB.sync(
	              it = extrack,
	              isBefore = isBefore,
	              canSee = canSee,
	              exclusiveCanSee = exclusiveCanSee)
      return (itB, itA)
    }


    def join(
      lmtrack:LmTrack[Bed],
      extrack:Iterator[Bed],
      isBefore:(Bed,Bed)=>Boolean = GenomeLocus.isBefore _,
      canSee:(Bed,Bed)=>Boolean = GenomeLocus.cond(GenomeLocus.Overlap(1)) _,
      exclusiveCanSee: Boolean = false) 
    : SyncedTrack[Bed,Bed] =
    {
      lmtrack.sync(
                it = extrack,
                isBefore = isBefore,
                canSee = canSee,
                exclusiveCanSee = exclusiveCanSee)
    }
 


    def joinN(n:Int)(
      lmtrack:LmTrack[Bed],
      extrack:Iterator[Bed])
    : SyncedTrack[Vector[Bed],Bed] =
    { //
      // Syncing lmtrack to sliding windows gp of size n 
      // of extrack. Each window is represented as a
      // vector of size n. 
      //
      // gp isBefore the current landmark y 
      // if the end of this sliding windown, viz. gp.last,
      // ends before y starts.
      //
      // gp canSee the current landmark y
      // if y is between the start and end of this sliding
      // window; i.e. y is between gp.head and gp.last.

      def isBefore(gp:Vector[Bed],y:Bed) = gp.last endBefore y
      def canSee(gp:Vector[Bed],y:Bed) = y.between(gp.head,gp.last)

      lmtrack.sync(
                it = SlidingIteratorN(n)(extrack),
                isBefore = isBefore,
                canSee = canSee,
                exclusiveCanSee = false)
    }
  } 
}




