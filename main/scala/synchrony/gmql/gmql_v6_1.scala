

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
    type OnSample[A] = DB.OnSample[A]
    type OnRegion[A] = DB.OnRegion[A]
    type GroupBySample[G] = DB.GroupBySample[G]
    type GroupByRegion[R] = DB.GroupByRegion[R]
    type CanJoinS = DB.CanJoinS
    type CanJoinR = DB.CanJoinR
    type Genometric = DB.Genometric
    type OutputS = DB.OutputS
    type OutputR = DB.OutputR
    type OrderR = DB.OrderR


    def select(
      sample:OnSample[SPred]  = DB.OnSample(),
      region:OnRegion[SBPred] = DB.OnRegion()) = DB.select(sample,region)(this)


    def extend(sample:OnSample[SProj]) = DB.extend(sample)(this)


    def project(
      sample:OnSample[SProj] = DB.OnSample(),
      region:OnRegion[BProj] = DB.OnRegion()) = DB.project(sample,region)(this)


    def flatGroupBy[G,R](
      sample:GroupBySample[G] = DB.NoSampleGroup,
      region:GroupByRegion[R] = DB.NoRegionGroup) = 
    {
      DB.flatGroupBy(sample,region)(this)
    }


    def groupbyS[G,A](grp:Sample=>G)(a:SGroupByProj[A]*) =
      DB.groupbyS(grp)(a:_*)(this)

    def groupbyR[R,B](grp:Bed=>R)(a:BGroupByProj[B]*) =
      DB.groupbyR(grp)(a:_*)(this)

    def partitionbyR[R,B](grp:Bed=>B)(a:BGroupByProj[B]*) =
      DB.partitionbyR(grp)(a:_*)(this)


    def map(
      region:OnRegion[BGroupByProj[Double]] = DB.OnRegion(), 
      joinby:OnSample[CanJoinS] = DB.OnSample())(edb:DB) = 
    {
      DB.map(region, joinby)(this,edb)
    }


    def difference(
      exact:Boolean = false,
      joinby:OnSample[CanJoinS] = DB.OnSample())(edb:DB) = 
    {
      DB.difference(exact,joinby)(this,edb)
    }


    def join(
      limit:Int=1000,
      pred:Genometric = DB.Genometric(), 
      joinbyS:OnSample[CanJoinS] = DB.OnSample(),
      joinbyR:OnRegion[CanJoinR] = DB.OnRegion(),
      outputS:DB.OutputS=DB.OverwriteS(),
      outputR:DB.OutputR=DB.BothR(),
      orderR:DB.OrderR=DB.DoneR)(edb:DB) =
    {
      DB.join(limit,pred,joinbyS,joinbyR,outputS,outputR,orderR)(this,edb)
    }


    def joinNearest(
      limit:Int = 200000,
      up:Int = 1,
      down:Int = 1,
      inclMid:Boolean = false,
      pred:Genometric = DB.Genometric(), 
      joinbyS:OnSample[CanJoinS] = DB.OnSample(),
      joinbyR:OnRegion[CanJoinR] = DB.OnRegion(),
      outputS:DB.OutputS=DB.OverwriteS(),
      outputR:DB.OutputR=DB.BothR(),
      orderR:DB.OrderR=DB.DoneR)(edb:DB) =
    {
      DB.joinNearest(
        limit, up, down, inclMid,
        pred, joinbyS, joinbyR,
        outputS, outputR, orderR)(this,edb)
    }
  }


  object DB 
  {
    import scala.language.implicitConversions

    type SPred = Predicates.SPred
    type SBPred = Predicates.SBPred
    type SProj = Projections.SProj
    type BProj = Projections.BProj
    type Aggr[A,B] = Projections.Aggr[A,B]
    type CanJoinS = Projections.CanJoinS
    type CanJoinR = Projections.CanJoinR
    case class OnSample[A](p:A*)
    case class OnRegion[A](p:A*)
    case class Genometric(g:LocusPred*)
    

    //
    // Emulation of GMQL's select statement.
    //


    def select(
      sample:OnSample[SPred]  = OnSample(),
      region:OnRegion[SBPred] = OnRegion())(db:DB) =
    {
      if (sample.p.isEmpty) db 
      else DB(
        for(s <- db.samples; if sample.p.forall(f => f(s))) 
        yield  
          if (region.p.isEmpty) s else s.updateT {
          for(r <- s.track(); if region.p.forall(f => f(s,r)))
          yield r }) 
    }


    //
    // Emulation of GMQL's extend
    //


    def extend(sample:OnSample[SProj])(db:DB) =
    {
      val (sproj, sprojAggr) = split(sample.p.to(Vector), Vector(), Vector())

      if (sproj.isEmpty) db else DB( for(s <- db.samples) yield  

      Step0{
        var snew = s
        val ps = sproj.iterator
        while (ps.hasNext) { 
          //
          // Remember that each projection may be defined on
          // other fields of the original sample s.
          // NOTE: GMQL's extend only permits projections that
          // are aggregate function. There is no good reason
          // to impose such a restriction. So I allow also 
          // non-aggregate functions as projections in extend.
         
          snew = snew.mergeM(ps.next()(s))
        }
        snew }.

      Step1{ snew =>
        if (sprojAggr.isEmpty) snew else
        { //
          // Remember that each projection may be defined on 
          // other fields of the original sample s.

          val aggr = mkAggrFromSProj(sprojAggr, s)
          snew mergeM (s.track().flatAggregateBy(aggr))
        } }.

      Done )
    }



    //
    // Emulation of GMQL's project.
    //


    def project(
      sample:OnSample[SProj]=OnSample(),
      region:OnRegion[BProj]=OnRegion())(db:DB) =
    {
      val (sproj, sprojAggr) = split(sample.p.to(Vector), Vector(), Vector())

      if (sproj.isEmpty) db else DB(for(s <- db.samples) yield

      Step0{
          //
          // Copy track but not meta data into snew
          //
        var snew = s.eraseM()
        val ps = sproj.iterator
        while (ps.hasNext) { 
          //
          // Apply projections which are non-aggregate functions
          // one by one. Remember that these may use meta data of
          // the original sample s.
          //
          snew = snew.mergeM(ps.next()(s))
        }
        snew }.

      Step1{ snew =>
        if (sprojAggr.isEmpty) snew else
        { // 
          // Aggregate functions sprojAggr may use meta data of the
          // original sample s, while the computed results have to
          // be merged into the projected sample.

          val aggr = mkAggrFromSProj(sprojAggr, s)
          snew mergeM (s.track().flatAggregateBy(aggr))
        } }.

      Step2{ snew => snew.updateT {
        for(r <- s.track()) yield
        { 
          var bed = r.eraseMisc()
          val ps = region.p.iterator
          while (ps.hasNext) {
            //
            // The tricky thing to remember is that the functions 
            // region.p may use meta data of the original sample s.

            bed = bed mergeMisc (ps.next()(s,r))
          }
          bed 
        } } }.

      Done )
    }


    //
    // Emulation of GMQL's groupby.
    //

    trait GroupBySample[+G]
    case object NoSampleGroup extends GroupBySample[Nothing]
    case class BySample[G](grp:SObj[G], aggr:SGroupByProj[Double]*)
    extends GroupBySample[G]

    trait GroupByRegion[+R]
    case object NoRegionGroup extends GroupByRegion[Nothing]
    case class ByRegion[R](grp:BObj[R], aggr:BGroupByProj[Double]*)
    extends GroupByRegion[R]


    def flatGroupBy[G,R](
      sample:GroupBySample[G] = NoSampleGroup, 
      region:GroupByRegion[R] = NoRegionGroup)(db:DB) =
    {
      def mkR[A](r:BObj[A]) = {
        //
        // In GMQL, regions are groupby locus by default. Not sure
        // why GMQL insists on this; perhaps this is the most
        // common use case. Even so, there is no reason to force-
        // couple locus to additional grouping keys that a user may
        // specify. Alas, this is what GMQL insists on...

        val g = r.asInstanceOf[BObj[String]] match {
                    case x:MkStringBObj => if (x.a=="") None else Some(r) 
                    case _ => Some(r) } 
        (u:Bed) => (u.chrom, u.chromStart, u.chromEnd, 
                    g match {
                        case Some(r) => Some(r(u)); 
                        case None => None })
      }

      Step0 { sample match {
        case NoSampleGroup => db
        case x:BySample[G] => 
        { 
          val b = mkAggrFromSGroupByProj(x.aggr.to(Vector))
 
          if (x.aggr.isEmpty) DB {
            (for ((g,acc)<-db.samples.groupby(x.grp(_))(keep); s<-acc)
             yield s mergeM (Map("group" -> g))).
            toVector }
          else DB {
            (for ((g,(c,acc))<-db.samples.groupby(x.grp(_))(withAcc(b));
                  s<-acc)
             yield s mergeM (Map("group" -> g) ++ c)).
            toVector } } } }.

      Step1{ db => region match{
        case NoRegionGroup => db 
        case x:ByRegion[R] =>
        {
          val (grp, ps) = (mkR(x.grp), x.aggr.to(Vector))

          val b = mkAggrFromBGroupByProj(ps)

          def mkgrp(u:Bed, g:Option[R]) = g match {
            case None => u
            case Some(x) => u.addMisc("group", x)
          }

          if (ps.isEmpty) { DB(for(s <- db.samples) yield 
            s updateT {
               (for((k,v)<-s.track().groupby(grp)(keep))
                yield { mkgrp(SimpleBedEntry(k._1,k._2,k._3), k._4)}).
               iterator})}

          else { DB(for(s <- db.samples) yield 
            s updateT {
               (for((k,v)<-s.track().groupby(grp)(b))
                yield { mkgrp(SimpleBedEntry(k._1,k._2,k._3, v), k._4)}).
               iterator})} } } }.

      Done
    }



    // I prefer groupby that retains a naturally nested grouping
    // structure. I also prefer groupby that does not assume
    // aggregate functions return Double. So here goes ...


    def groupbyS[G,A](grp:Sample=>G)(a:SGroupByProj[A]*)(db:DB) =
    { //
      // This is Synchrony's general groupby. It follows
      // the natural nested structure of grouping.

      val bs= a.to(Vector).map({case SGroupByProj(k,f)=>f.sa |>((x:A)=>(k,x))})
      def mix(bs:Array[(String,A)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)
      
      if (a.to(Vector).isEmpty) db.samples.groupby(grp)(keep)
      else db.samples.groupby(grp)(b)
    }


    def groupbyR[R,B](grp:Bed=>R)(a:BGroupByProj[B]*)(db:DB) =
    { //
      // This is Synchrony's generalized groupby on region/track.
      // It follows the natural nested structure of groups.
      // A user can later process each resulting group in
      // whatever ways he like, w/o forcing the groups into
      // Bed/track right now.

      val bs= a.to(Vector).map({case SProjBAggr(k,f)=>f.a |> ((x:B)=>(k,x))})
      def mix(bs:Array[(String,B)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      for(s <- db.samples) 
      yield if (a.to(Vector).isEmpty) (s, s.track().groupby(grp)(keep))
            else (s, s.track().groupby(grp)(b))
    }


    def partitionbyR[R,B](grp:Bed=>B)(a:BGroupByProj[B]*)(db:DB) =
    { //
      // Similar to groupbyR, but assumes that the entries in
      // each group are sorted in a way consistent with "isBefore".

      val bs= a.to(Vector).map({case SProjBAggr(k,f)=>f.a |> ((x:B)=>(k,x))})
      def mix(bs:Array[(String,B)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(mix, bs:_*)

      for(s <- db.samples) 
      yield if (a.to(Vector).isEmpty) (s, s.track().partitionby(grp)(keep))
            else (s, s.track().partitionby(grp)(b))
    }



    //
    // Emulation of GMQL's map.
    //


    def map(
      region:OnRegion[BGroupByProj[Double]] = OnRegion(), 
      joinby:OnSample[CanJoinS] = OnSample())(rdb:DB, edb:DB) =
    { 
      val b= mkAggrFromBGroupByProj(
        SProjBAggr("count", Count) +: region.p.to(Vector))

      def mkname(s:Sample,d:String) = 
        d + (if (s.hasM("name")) s.getM("name") else "")

      Step0{
        mapS(joinby.p:_*)(rdb, edb) }.

      Step1{ db=> for((ref,matches) <- db; e <- matches) yield {
        val refN= ref.renameM(mkname(ref, "ref.") + _)
        val eN= e.renameM(mkname(e, "expt.") + _)
        val lm= LmTrack(refN.track())
        val ex= Connectors.join(lm, eN.track())  
        val tr= for(r<-lm) yield r.mergeMisc((ex syncWith r).flatAggregateBy(b))
        refN.overwriteM(eN.meta).updateT(tr) } }. 

      Done
    }


    private def mapS(joinby:CanJoinS*)(rdb:DB, edb:DB) =
    { //
      // GMQL has an unnecessarily restricted join predicate,
      // basically canJoinS(p1, p2, ...) where p1, p2, ...
      // are meta attributes, meaning two samples can join
      // if they agree on these meta attributes.
      //
      // In Synchrony, we allow more general join predicates,
      // viz. any Boolean function on a pair of samples.

      def canJoin(ref:Sample, e:Sample) = joinby.forall(p=> p(ref, e))

      (for(ref<-rdb.samples; e<-edb.samples; if (canJoin(ref,e)))
       yield (ref, e)) .
      groupby(_._1)(OpG.map(_._2)) .to(Vector)
    }

 

    //
    // Emulaton of GMQL's difference.
    //


    def difference(
      exact:Boolean = false,
      joinby:OnSample[CanJoinS] = OnSample())(rdb:DB, edb:DB) =
    {
      def overlap(x:Bed,y:Bed)= if (exact) {x sameLocus y} else x.overlap(1)(y)

      Step0{
        mapS(joinby.p:_*)(rdb, edb) }.

      Step1{ db => 
        for((ref, diffs) <- db;
            lm = LmTrack(ref.track());
            ds = for(d<-diffs) yield Connectors.join(lm,d.track());
            tr = for(r <- lm; 
                     if ds.forall(d=>(d syncWith r).forall(x=> !overlap(r,x))))
                 yield r;
            if (tr.hasNext))
        yield ref updateT tr }. 

      Done
    }



    //
    // Auxiliary functions for emulating GMQl's syntax
    // for project, extend,and groupby.

    private def split(
      ps:Vector[SProj],
      x :Vector[SProj],
      y :Vector[SProj])
    : (Vector[SProj], Vector[SProj]) =
    { //
      // Split up a list of projections into
      // aggregate and non-aggregate functions.

      if (ps.isEmpty) (x, y) else ps.head match {
        case p:SPObj[_]       => split(ps.tail, x :+ p, y)
        case p:SProjSimple[_] => split(ps.tail, x :+ p, y)
        case p:SProjGen[_]    => split(ps.tail, x :+ p, y)
        case p:SProjBAggr[_]  => split(ps.tail, x, y :+ p)
        case p:SProjSBAggr[_] => split(ps.tail, x, y :+ p)
      }
    }


    private def mkAggrFromSProj(ps:Vector[SProj], s:Sample) =
    { //
      // Combine multiple independent aggregate functions
      // on regions/track into a single aggregate function, 
      // so that the input track is scanned only once.
      // NOTE: As required by GMQL, the aggregate functions
      // are assumed to produce Double as results.
  
      def cvt[A](a:Aggr[Bed,A]) = a.asInstanceOf[Aggr[Bed,Double]]
      val b=ps.map({
              case SProjSBAggr(k,sa)=> cvt(sa.sa(s)) |> ((x:Double)=>(k,x))
              case SProjBAggr(k,a)  => cvt(a.a) |> ((x:Double)=>(k,x))})
      def mix(b:Array[(String,Double)]) = Map(b.toIndexedSeq:_*)
      combineN(mix, b:_*)
    }

    
    private def mkAggrFromBGroupByProj(ps:Vector[BGroupByProj[Double]]) =
    { 
      def cvt[A](a:Aggr[Bed,A]) = a.asInstanceOf[Aggr[Bed,Double]]
      val b=ps.map({case SProjBAggr(k,a)=> cvt(a.a) |> ((x:Double)=>(k,x))})
      def mix(b:Array[(String,Double)]) = Map(b.toIndexedSeq:_*)
      combineN(mix, b:_*)
    }
 

    private def mkAggrFromSGroupByProj(as:Vector[SGroupByProj[Double]]) =
    { 
      val bs= as.to(Vector).
              map({case SGroupByProj(k,f)=>f.sa.|>((x:Double)=>(k,x))})
      def mix(bs:Array[(String,Double)]) = Map(bs.toIndexedSeq:_*)
      combineN(mix, bs:_*)
    }



    //
    // Emulation of GMQL's join
    //


    def join(
      limit:Int=1000,
      pred:Genometric=Genometric(), 
      joinbyS:OnSample[CanJoinS] = OnSample(),
      joinbyR:OnRegion[CanJoinR] = OnRegion(),
      outputS:OutputS = OverwriteS(),
      outputR:OutputR = BothR(),
      orderR:OrderR = DoneR)(rdb:DB, edb:DB) =
    {
      Step0{
        mapS(joinbyS.p:_*)(rdb, edb) }.

      Step1{ db =>
      { //
        // NOTE: GMQL automatically renames keys of meta data
        // before samples are merged/joined. Here, I dont 
        // automatically rename; users can invoke renameM() on
        // samples explicitly to do the renaming themselves.
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

        def canJoinR(r:Bed, t:Bed) = joinbyR.p.forall(f=> f(r, t))

        for((anchor, expts) <- db;
            e <- expts;
            lm = LmTrack(anchor.track());
            ex = Connectors.join(
                   lmtrack=lm,
                   extrack=e.track(),
                   canSee=GenomeLocus.cond(GenomeLocus.DLE(limit)) _);
            tr = for(r <- lm;
                     t <- ex syncWith r;
                     if (canJoinR(r, t));
                     if (GenomeLocus.cond(pred.g:_*)(t,r));
                     joined <- outputR(r,t))
                 yield joined)
        yield outputS(anchor, e).updateT(orderR(tr))
      } }.

      Done
    }
      

    //
    // joinNearest is not very efficiently implemented.
    // Will fix later.

    def joinNearest(
      limit:Int = 200000,
      up:Int = 1,
      down:Int = 1,
      inclMid:Boolean = false,
      pred:Genometric=Genometric(), 
      joinbyS:OnSample[CanJoinS] = OnSample(),
      joinbyR:OnRegion[CanJoinR] = OnRegion(),
      outputS:OutputS = OverwriteS(),
      outputR:OutputR = BothR(),
      orderR:OrderR = DoneR)(rdb:DB, edb:DB) =
    {
      Step0{
        mapS(joinbyS.p:_*)(rdb, edb) }.

      Step1{ db =>

        def nearest(r:Bed, ts:Vector[Bed]) =
        {
          def grp(x:Bed) = (x endBefore r, x distFrom r)
          val groups = (ts.partitionby(grp)(keep)).toVector
          val (u, md) = groups.span(_._1._1)
          val (m, d) = md.span(_._1._2 == 0)
          def get(n:Int, groups:Vector[(Int,Vector[Bed])]) = 
            for (ts<- groups.sortBy(_._1).take(n); t<-ts._2) yield t
          def upstr = get(up, for(((p,d),v) <- u) yield (d,v))
          def downstr = get(down, for(((p,d),v) <- d) yield (d,v))
          def midstr = for(((p,d),v)<-m; t<-v) yield t
          upstr ++ { if (inclMid) midstr else Vector() } ++ downstr
        }

        def canJoinR(r:Bed, t:Bed) = joinbyR.p.forall(f=> f(r, t))

        for((anchor, expts) <- db;
            e <- expts;
            lm = LmTrack(anchor.track());
            ex = Connectors.join(
                   lmtrack=lm,
                   extrack=e.track(),
                   canSee=GenomeLocus.cond(GenomeLocus.DLE(limit)) _);
            tr = for(r <- lm;
                     ts = ex syncWith r;
                     t <- nearest(r, ts);
                     if (canJoinR(r, t));
                     if (GenomeLocus.cond(pred.g:_*)(t,r));
                     joined <- outputR(r,t))
                 yield joined)
        yield outputS(anchor, e).updateT(orderR(tr)) }.

      Done
    }


    //
    // Here are some auxiliary functions to emulate
    // GMQL's more restricted/specialised join
    // predicates and output formats.
    //

    
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
      def apply(it:Iterator[Bed]) = it.to(Vector).sorted.iterator
    }

    case object SortDistinctR extends OrderR {
      def apply(it:Iterator[Bed]) = DistinctR(SortR(it))
    }
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
      // if y ends between the start and end of this sliding
      // window; i.e. y is between gp.head and gp.last.

      def isBefore(gp:Vector[Bed],y:Bed) = gp.last endBefore y

      def canSee(gp:Vector[Bed],y:Bed) = 
        ((gp.head isBefore y) && (y isBefore gp.last)) ||
        (gp.exists(x => x.overlap(0)(y))) 

      lmtrack.sync(
                it = SlidingIteratorN(n)(extrack),
                isBefore = isBefore,
                canSee = canSee,
                exclusiveCanSee = false)
    }
  } 
}




