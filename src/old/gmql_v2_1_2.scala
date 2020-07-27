

package synchrony.gmql

/** GenomMetric Query Language (GMQL)
 *
 *  This module is intended primarily as a stress test for Synchrony 
 *  iterators, by implementing GMQL, a fairly complex and demanding
 *  domain-specific language for querying Bed-like files. All the key
 *  features of GMQL are implemented, except for
 *
 *   (i) the Cover function; and 
 *  (ii) GMQL provides the notions of start/end and left/right 
 *      describing the two ends of a genomic region, whereas this
 *      implementation provides only the left/right notion and calls
 *      them start/end --- due to misunderstanding of GMQL documentation.
 *  
 * Wong Limsoon
 * 13 May 2020
 *
 */



object GMQL {

  import synchrony.gmql.Predicates._
  import synchrony.gmql.Projections._
  import synchrony.gmql.Samples._
  import synchrony.gmql.Samples.implicits._
  import synchrony.gmql.Samples.SampleFile._
  import synchrony.gmql.Samples.SampleFile.OpG.keep
  import synchrony.programming.StepContainer._
  import synchrony.programming.Sri.{combineN, withAcc} 


  var DEBUG = true
  var SWITCHTHRESHOLD = 2

  val GenomeLocus = synchrony.genomeannot.GenomeAnnot.GenomeLocus
  val Bed     = synchrony.gmql.Samples.Bed
  val OpG     = synchrony.gmql.Samples.SampleFile.OpG
  val Samples  = synchrony.gmql.Samples
  val Predicates = synchrony.gmql.Predicates
  val Projections = synchrony.gmql.Projections

  type Meta   = Samples.Meta
  type Track  = Samples.BedEIterator
  type Sample = Samples.Sample
  type SampleFileSettings = Samples.SampleFileSettings
  type Bed = Samples.Bed
  type LocusPred = GenomeLocus.LocusPred



  /** GMQL queries are generally functions that accept a DB of Samples
   *  as input and produces a DB of Samples as output. So DB of Samples
   *  is the main class in our implementation, and GMQL query functions
   *  are methods of the class.
   */
  
  case class DB(samples:SampleFile) {

    type SPred = DB.SPred
    type SBPred = DB.SBPred
    type SProj = DB.SProj
    type OnSample[A] = DB.OnSample[A]
    type OnRegion[A] = DB.OnRegion[A]
    type GroupBySample[G] = DB.GroupBySample[G]
    type BySample[G] = DB.BySample[G]
    type GroupByRegion[R] = DB.GroupByRegion[R]
    type ByRegion[R] = DB.ByRegion[R]
    type CanJoinS = DB.CanJoinS
    type CanJoinR = DB.CanJoinR
    type Genometric = DB.Genometric
    type OutputS = DB.OutputS
    type OutputR = DB.OutputR
    type OrderR = DB.OrderR


  //
  // GMQL query operators
  //
  //
  // The syntax of GMQL select query is:
  //
  // GMQL   ::= select(sample = OnSample(SPred*), region = OnRegion(SBPred*))
  //
  //         |  extend(sample = OnSample(SProj*)) 
  //
  //         |  project(sample = OnSample(SProj*), region = OnRegion[BProj*])
  //
  //         |  flatGroupBy(sample = GroupBySample, region = GroupByRegion)
  //         |
  //         |  map(region = OnRegion(BGroupByProj), 
  //                joinby = OnSample(CanJoinS))(edb:DB)
  //
  //         |  difference(joinby = OnSample(CanJoinS), exact:Boolean)(edb:DB)
  //
  //         |  join(limit:Int, pred:Genometric,
  //                 joinbyS = OnSample(CanJoinS), joinbyR = OnRegion(CanJoinR),
  //                 outputS = OutputS, outputR = OutputR,
  //                 orderR = OrderR)(edb:DB)
  //
  //         |  join(limit:Int, up:Int, down:Int, inclMid:Boolean,
  //                 pred:Genometric,
  //                 joinbyS = OnSample[CanJoinS], joinbyR = OnRegion[CanJoinR],
  //                 outputS = OutputS, outputR = OutputR,
  //                 orderR = OrderR)(edb:DB)
  //
  // SBPred ::= SBPred and SBPred | SBPred or SBPred | not SBPred 
  //         |  SPred | BPred | f:(Sample, Bed) => Boolean | f:Sample => BPred
  //         |  SBObj cop SBObj | SObj cop BObj | BObj cop SObj
  //
  // SPred  ::= SPred and SPred | SPred or SPred | not SPred 
  //         |  SemiJoin(incl: Incl, excl: Excl, exDB: DB)
  //         |  f:Sample => Boolean | SObj cop SObj
  //
  // BPred  ::= BPred and BPred |  BPred or BPred |  not BPred
  //         |  f:Bed => Boolean |  BObj cop BObj
  //
  // SProj  ::= k:String as SObj | k:String as SBAggr | k:String as BAggr 
  //         | k:String as f:Sample => A
  //
  // BProj  ::= k:String as BObj | k:String as SBObj | k:String as VSAggr 
  //         | k:String as f:Bed => A | k:String as f:(Sample, Bed) => A
  //
  // SObj   ::= SObj aop SObj | SBAggr | BAggr | MetaS[A](k: String)
  //         |  a:Double | a:Int | a:Boolean | a:String
  //
  // SBAggr ::= Average of SBObj | Sum of SBObj | Biggest of SBObj 
  //         |  Smallest of SBObj | Count 
  //         |  BaseAggrOverRegions[A](f:Sri[Bed, A])
  //         |  GenAggrOverRegions[A](f:(Bed => A) => Sri[Bed, A])
  //
  // BAggr  ::= Average of BObj | Sum of BObj | Biggest of BObj 
  //         |  Smallest of BObj | Count
  //         |  BaseAggrOverRegions[A](f:Sri[Bed, A])
  //         |  GenAggrOverRegions[A](f:(Bed => A) => Sri[Bed, A])
  //
  // VSAggr ::= Average of SObj | Sum of SObj | Biggest of SObj 
  //         |  Smallest of SObj | Count
  //         |  BaseAggrOverSamples[A](f:Sri[Sample, A])
  //         |  GenAggrOverSamples[A](f:(Sample => A) => Sri[Sample, A])
  //
  // Incl   ::= Incl(f: String*)
  // Excl   ::= Excl(f: String*)
  //
  // SBObj  ::= SBObj aop SBObj | SObj aop BObj | BObj aop SObj 
  //         |  f:(Sample, Bed) => A | a:Double | a:Int | a:Boolean | a:String
  //
  // BObj   ::= BObj aop Bobj 
  //         |  Chr | Start | End | Strand | Score | Name | MetaR[A](k: String)
  //         |  f:Bed => A | a:Double | a:Int | a:Boolean | a:String 
  //
  // cop    ::= === | !== | >= | > | <= | <
  // aop    ::= + | - | * | /
  //
  // GroupBySample ::= NoSampleGroup
  //                |  BySample(grp = SObj, aggr = SGroupByProj*)
  //
  // SGroupByProj  ::= k:String as VSAggr
  //
  // GroupByRegion ::= NoRegionGroup
  //                |  ByRegion(grp = BObj, aggr = BGroupByProj*) 
  //
  // BGroupByProj  ::= k:String as BAggr
  //
  // CanJoinS ::= p:String | f:(Sample, Sample) => Boolean
  //
  // CanJoinR ::= p:String | f:(Bed, Bed) => Boolean
  //
  // OutputS  ::=
  //
  // OutputR  ::=
  //
  // OrderR   ::=
  //


    def select(
      sample: OnSample[SPred]  = DB.OnSample(),
      region: OnRegion[SBPred] = DB.OnRegion()): 
    DB = {

      DB.select(sample, region)(this)

    }



    def extend(sample: OnSample[SProj]): DB = DB.extend(sample)(this)
    


    def project(
      sample: OnSample[SProj] = DB.OnSample(),
      region: OnRegion[BProj] = DB.OnRegion()): 
    DB = {

      DB.project(sample, region)(this)

    }
    


    def flatGroupBy[G, R](
      sample: GroupBySample[G] = DB.NoSampleGroup,
      region: GroupByRegion[R] = DB.NoRegionGroup): 
    DB = {

      DB.flatGroupBy(sample,region)(this)

    }



    def flatGroupByS[G](sample: BySample[G]): DB = DB.flatGroupByS(sample)(this)



    def flatGroupByR[R](region: ByRegion[R]): DB = DB.flatGroupByR(region)(this)


    def flatPartitionByR[R](region: ByRegion[R]): DB = {

      DB.flatPartitionByR(region)(this)

    }



/*
 * Omit for now.
 *
    def groupbyS[G, A]
      (grp: Sample => G)
      (a: SGroupByProj[A]*) 
    : scala.collection.mutable.Map[G, Map[String, A]] =
    {
      DB.groupbyS(grp)(a:_*)(this)
    }


    def groupbyR[R, B]
      (grp: Bed => R)
      (a: BGroupByProj[B]*)
    : Iterator[(Sample, scala.collection.mutable.Map[R, Map[String, B]])] =
    {
      DB.groupbyR(grp)(a:_*)(this)
    }


    def partitionbyR[R, B]
      (grp: Bed => R)
      (a: BGroupByProj[B]*)
    : Iterator[(Sample, Iterator[(R, Map[String, B])])] = {

      DB.partitionbyR(grp)(a:_*)(this)
    }

 *
 * End omission.
 */



    def map
      (region: OnRegion[BGroupByProj[Double]] = DB.OnRegion(), 
       joinby: OnSample[CanJoinS] = DB.OnSample())
      (edb: DB): 
    DB = {

      DB.map(region, joinby)(this,edb)

    }



    def difference
      (joinby: OnSample[CanJoinS] = DB.OnSample(),
       exact: Boolean = false)
      (edb: DB): 
    DB =  {

       DB.difference(joinby, exact)(this, edb)

    }



    def join
      (limit:   Int                = 1000,
       pred:    Genometric         = DB.Genometric(), 
       joinbyS: OnSample[CanJoinS] = DB.OnSample(),
       joinbyR: OnRegion[CanJoinR] = DB.OnRegion(),
       outputS: OutputS            = DB.OverwriteS(),
       outputR: OutputR            = DB.BothR(),
       orderR: DB.OrderR           = DB.DoneR)
      (edb: DB): 
    DB = (

      DB.join
        (limit, pred, joinbyS, joinbyR, outputS, outputR, orderR)
        (this, edb)

    )
    


    def joinNearest
      (limit:   Int                = 100000,
       up:      Int                = 1,
       down:    Int                = 1,
       inclMid: Boolean            = false,
       pred:    Genometric         = DB.Genometric(), 
       joinbyS: OnSample[CanJoinS] = DB.OnSample(),
       joinbyR: OnRegion[CanJoinR] = DB.OnRegion(),
       outputS: OutputS            = DB.OverwriteS(),
       outputR: OutputR            = DB.BothR(),
       orderR: OrderR              = DB.DoneR)
      (edb: DB): 
    DB =  (

      DB.joinNearest
        (limit, up, down, inclMid, pred, 
         joinbyS, joinbyR, outputS, outputR, orderR)
        (this, edb)

    )




  //
  // Operations for managing DB.
  //


  /** Sort the tracks of Samples in this DB.
   *
   *  Our implementation of GMQL queries is based on Synchrony
   *  iterators. Synchrony iterators are designed to operate on
   *  sorted data files / streams. 
   *
   *  @param ordering is the ordering used to sorting the tracks
   *     of Samples in this DB.
   *  @return a copy of this DB with the tracks of its Samples sorted.
   */

    def tracksSorted
      (implicit ordering: Ordering[Bed] = Sample.ordering)
    : DB = DB.sortTracks(this)(ordering)

  

  /** Save this DB.
   *
   *  @param filename is name of the file this DB is saved as.
   */

    def savedAs(filename: String) = DB.saveAs(this, filename)


  /** Queries are lazy. I.e., query results are kept on iterator, 
   *  and are produced on-demand and are accessible once only.
   *  To get all results in one go, and retain them, you need to 
   *  materialize the results either in memory or on disk. You can
   *  also let the system decides!
   */

    def materializedOnDisk: DB = DB.materializeOnDisk(this)

    def materializedInMemory: DB = DB.materializeInMemory(this)

    def materialized: DB = DB.materialize(this)

  } // End class DB.



  /** Implementation of GMQL queries
   */

  object DB {

    type SPred     = Predicates.SPred
    type SBPred    = Predicates.SBPred
    type SProj     = Projections.SProj
    type BProj     = Projections.BProj
    type Aggr[A,B] = Projections.Aggr[A,B]
    type CanJoinS  = Projections.CanJoinS
    type CanJoinR  = Projections.CanJoinR

    case class OnSample[A](p:A*)
    case class OnRegion[A](p:A*)
    case class Genometric(g:LocusPred*)
    

  /** Construct a DB from file. The file is a SampleFile 
   *  formatted on disk in Limsoon's format, as described
   *  in synchrony.gmql.Samples.
   *
   *  @param samplelist is the filename.
   *  @return the constructed DB.
   */

    def apply(samplelist: String): DB = onDiskDB(samplelist) 


  /** Construct a DB from file. The file is a SampleFile 
   *  formatted on disk in Limsoon's format, as described
   *  in synchrony.gmql.Samples.
   *
   *  @param samplelist is the name of the SampleFile on disk.
   *  @settings is the custom-made settings to use for the SampleFile
   *  @return the constructed DB.
   */

    def onDiskDB(
      samplelist: String,
      settings:   SampleFileSettings = defaultSettingsSampleFile,
      nocheck:    Boolean = false): 
    DB =  {

      new DB(onDiskSampleFile(samplelist, settings, nocheck))

    }
    


    def onDiskEncodeNPDB(
      samplelist: String,
      settings:   SampleFileSettings = encodeNPSettingsSampleFile,
      nocheck:    Boolean = false): 
    DB =  {

      new DB(onDiskSampleFile(samplelist, settings, nocheck))

    }

    


  /** Construct a DB from file. The file is a SampleFile 
   *  formatted on disk in Stefano's format, as described
   *  in synchrony.gmql.Samples.
   *
   *  @param path is the folder containing the Sample files.
   *  @param samplelist is the name of the SampleFile on disk.
   *  @settings is the settings to use for the SampleFile
   *  @return the constructed DB.
   */

    def altOnDiskDB
      (path:       String)
      (samplelist: String,
       settings:   SampleFileSettings = altSettingsSampleFile(path)): 
    DB = {

      new DB(altOnDiskSampleFile(path)(samplelist, settings))

    }

    
    

  /** Construct a DB from file. The file is an SampleFile 
   *  of ENCODE Narrow Peaks formatted on disk in Stefano's format,
   *  as described in synchrony.gmql.Samples.
   *
   *  @param dbpath is the folder containing the Sample files.
   *  @param samplelist is the name of the SampleFile on disk.
   *  @settings is the settings to use for the SampleFile
   *  @return the constructed DB.
   */

    def altOnDiskEncodeNPDB
      (path:       String)
      (samplelist: String,
       settings:   SampleFileSettings = altSettingsEncodeNPSampleFile(path)):
    DB = {

      altOnDiskDB(path)(samplelist, settings)

    }



  /** Queries are lazy. I.e., query results are kept on iterator, 
   *  and are produced on-demand and are accessible once only.
   *  To get all results in one go, and retain them, you need to 
   *  materialize the results either in memory or on disk. You can
   *  also let the system decides!
   */

    def materializeOnDisk(db: DB): DB   = DB(db.samples.serialized)

    def materializeInMemory(db: DB): DB = DB(db.samples.eiterator.toVector)

    def materialize(db: DB): DB         = DB(db.samples.stored)

    

  /** Save a DB.
   *
   *  TODO: Currently saving DB in a simple-minded way,
   *  which breaks easily. Need more flexible/robust way.
   *
   *  @param db is a DB
   *  @param filename is a filename.
   *  @return the saved DB.
   */
    
    def saveAs(db: DB, filename: String):DB = DB(db.samples.savedAs(filename))



  /** Construct a DB from a vector of Sample
   *
   *  @param samples is the vector of Sample.
   *  @return the constructed DB.
   */

    def apply(samples: Iterable[Sample]): DB   = inMemoryDB(samples.toVector)

    def inMemoryDB(samples: Vector[Sample]):DB = DB(inMemorySampleFile(samples))



  /** Construct a DB from an iterator of Sample
   *
   *  @param samples is the iterator of Sample.
   *  @return the constructed DB.
   */

    def apply(samples: Iterator[Sample]): DB       = transientDB(samples)

    def transientDB(samples: Iterator[Sample]): DB = {

      DB(transientSampleFile(samples))

    }




  /** Sort the tracks of Samples in this DB.
   *
   *  Our implementation of GMQL queries is based on Synchrony
   *  iterators. Synchrony iterators are designed to operate on
   *  sorted data files / streams. 
   *
   *  @param ordering is the ordering used to sorting the tracks
   *     of Samples in this DB.
   *  @ return a copy of this DB with the tracks of its Samples sorted.
   */

    def sortTracks
      (db: DB)
      (implicit ordering: Ordering[Bed] = Sample.ordering)
    : DB = {

      DB { 
        for (s <- db.samples.eiterator) 
        yield s.trackSorted(ordering)
      }

    }





  /** Emulation of GMQL's select.
   */

    def select
      (sample: OnSample[SPred]  = OnSample(),
       region: OnRegion[SBPred] = OnRegion())
      (db: DB): 
    DB = {

      (sample.p.isEmpty, region.p.isEmpty) match {

        case (true, true)  => db 

        case (false, true) => DB { 
          db.samples.filtered(s => sample.p.forall(f => f(s)))
        }

        case (true, false) => DB { 
          for (s <- db.samples.eiterator;
               tr = s.slurpedTrack.filter(r => region.p.forall(f => f(s, r))))
          yield s.trackUpdated { tr }
        }

        case _ => DB { 
          for (s <- db.samples.eiterator; if sample.p.forall(f => f(s));
               tr = s.slurpedTrack.filter(r => region.p.forall(f => f(s, r))))
          yield s.trackUpdated { tr }
        }
      }
    }



  /** Emulation of GMQL's extend
   */

    def extend
      (sample: OnSample[SProj])
      (db: DB): 
    DB = {

      val (sproj, sprojAggr) = split(sample.p.to(Vector), Vector(), Vector())
      val projs = sproj.map(_.toPair)
      val aggrs = mkAggrFromSProj(sprojAggr) _

      (sproj.isEmpty, sprojAggr.isEmpty) match {

        case (true, true)  => db

        case (false, true) => DB {
          for (s <- db.samples.eiterator)
          yield s.mOverwrittenWith(projs:_*) 
        }

        case (true, false) => DB {
          for (s <- db.samples.eiterator)
          yield s.mOverwritten(s.bedFile.flatAggregateBy(aggrs(s)))
        }

        case _ => DB {
          for (s <- db.samples.eiterator)
          yield s.mOverwrittenWith(projs:_*) 
                 .mOverwritten(s.bedFile.flatAggregateBy(aggrs(s)))
        }
      }
    }



  /** Emulation of GMQL's project.
   */

    def project
      (sample: OnSample[SProj] = OnSample(),
       region: OnRegion[BProj] = OnRegion())
      (db: DB): 
    DB = DB {

      val (sproj, sprojAggr) = split(sample.p.to(Vector), Vector(), Vector())
      val projs = sproj.map(_.toKV)
      val aggrs = mkAggrFromSProj(sprojAggr) _

      val projr = region.p.to(Vector).map(_.toSUKV)
      val conflict = projr.exists { case (k, _) =>
        k == "chrom" || k == "chromstart" || k == "chromEnd" ||
        k == "name"  || k == "score"      || k == "strand"
      }

      for (s <- db.samples.eiterator;
           newS = (sproj.isEmpty, sprojAggr.isEmpty) match {
             case (true, true)  => s.mErased
             case (false, true) => s.mErased.mMerged(Map(projs :_*))
             case (true, false) => s.mErased
                                    .mOverwritten(
                                      s.bedFile.flatAggregateBy(aggrs(s)))
             case (false, false)=> s.mErased
                                    .mMerged(Map(projs :_*))
                                    .mOverwritten(
                                      s.bedFile.flatAggregateBy(aggrs(s))) })
      yield (projr.isEmpty, conflict) match {
        case (true, _)      => newS 
        case (false, true)  => newS.trackUpdated {
          for (r <- s.track; m = Map(projr :_*))
          yield r.eraseMisc().overwriteMisc(m)
        }
        case (false, false) => newS.trackUpdated {  
          for (r <- s.track; m = Map(projr :_*))
          yield r.eraseMisc().simpleOverwriteMisc(m)
        }
      }
    }




  /** Emulation of GMQL's groupby.
   */

    sealed trait GroupBySample[+G]
    final case object NoSampleGroup extends GroupBySample[Nothing]
    final case class BySample[G](grp: SObj[G], aggr: SGroupByProj[Double]*)
    extends GroupBySample[G]

    sealed trait GroupByRegion[+R]
    case object NoRegionGroup extends GroupByRegion[Nothing]
    case class ByRegion[R](grp:BObj[R], aggr:BGroupByProj[Double]*)
    extends GroupByRegion[R]


    def flatGroupBy[G, R]
      (sample: GroupBySample[G] = NoSampleGroup, 
       region: GroupByRegion[R] = NoRegionGroup)
      (db: DB): 
    DB = (sample, region) match {

      case (NoSampleGroup, NoRegionGroup)   => db

      case (x: BySample[G], NoRegionGroup)  => flatGroupByS(x)(db)

      case (NoSampleGroup, y: ByRegion[R])  => flatGroupByR(y)(db)

      case (x: BySample[G], y: ByRegion[R]) =>
        flatGroupByR(y)(flatGroupByS(x)(db))
    }


    def flatGroupByS[G](sample: BySample[G])(db: DB): 
    DB =  DB {

      val b = mkAggrFromSGroupByProj(sample.aggr.to(Vector))

      if (sample.aggr.isEmpty) 
        for ((g, acc) <- db.samples.groupby(sample.grp(_))(keep); s <- acc)
        yield s.mOverwritten(Map("group" -> g))

      else 
        for ((g, (c, acc)) <- db.samples.groupby(sample.grp(_))(withAcc(b));
              s <-acc)
        yield s.mOverwritten(Map("group" -> g) ++ c)
    }


    def flatGroupByR[R](region: ByRegion[R])(db: DB): 
    DB = DB {

      def loci(u: Bed) = (u.chrom, u.chromStart, u.chromEnd) 

      def extraGrp[A](r: BObj[A]) = r match {
        case x:MkStringBObj => 
          if (x.a == "") None 
          else Some(x.a, (u: Bed) => r(u))
        case _ => Some("group", (u: Bed) => r(u))
      }

      def mkGrp[A](r: Bed => A) = (u: Bed) => 
        (u.chrom, u.chromStart, u.chromEnd, r(u))
 
      val ps = region.aggr.toVector
      val aggr = mkAggrFromBGroupByProj(ps)

      (ps.isEmpty, extraGrp(region.grp)) match { 

        case (true, None) =>
          for (s <- db.samples.eiterator)
          yield 
            s.trackUpdated {
              for((k, v) <- s.track.groupby(loci)(keep))
              yield Bed(chrom = k._1, chromStart = k._2, chromEnd = k._3)
            }


        case (true, Some(grp)) =>
          for (s <- db.samples.eiterator)
          yield
            s.trackUpdated {
              for((k, v) <- s.track.groupby(mkGrp(grp._2))(keep))
              yield Bed(chrom = k._1, 
                        chromStart = k._2, 
                        chromEnd = k._3
                    ).addMisc(grp._1, k._4)
            }

        case (false, None) =>
          for (s <- db.samples.eiterator)
          yield
            s.trackUpdated {
              for((k, v) <- s.track.groupby(loci)(aggr))
              yield Bed(chrom = k._1, 
                        chromStart = k._2,
                        chromEnd = k._3
                    ).overwriteMisc(v)
            }

        case (false, Some(grp)) =>
          for (s <- db.samples.eiterator)
          yield
            s.trackUpdated {
                     for((k, v) <- s.track.groupby(mkGrp(grp._2))(aggr))
                     yield Bed(chrom = k._1, 
                               chromStart = k._2,
                               chromEnd = k._3
                           ).addMisc(grp._1, k._4).overwriteMisc(v)
            }
      }
    }
    .tracksSorted



    def flatPartitionByR[R](region: ByRegion[R])(db: DB): 
    DB = DB {

      def loci(u: Bed) = (u.chrom, u.chromStart, u.chromEnd) 

      def extraGrp[A](r: BObj[A]) = r match {
        case x:MkStringBObj => 
          if (x.a == "") None 
          else Some(x.a, (u: Bed) => r(u))
        case _ => Some("group", (u: Bed) => r(u))
      }

      val ps = region.aggr.toVector
      val aggr = mkAggrFromBGroupByProj(ps)

      (ps.isEmpty, extraGrp(region.grp)) match {

        case (true, None) =>
          for (s <- db.samples.eiterator)
          yield s.trackUpdated {
            for((k, v) <- s.track.partitionby(loci)(keep))
            yield Bed(chrom = k._1, chromStart = k._2, chromEnd = k._3)
          }

        case (true, Some(grp)) =>
          for (s <- db.samples.eiterator)
          yield s.trackUpdated {
            for((k, v) <- s.track.partitionby(loci)(keep);
                (g, _) <- v.groupby(grp._2)(keep)) 
            yield Bed(chrom = k._1, 
                      chromStart = k._2,
                      chromEnd = k._3
                  ).addMisc(grp._1, g)
          }

        case (false, None) =>
          for (s <- db.samples.eiterator)
          yield s.trackUpdated {
            for((k, v) <- s.track.partitionby(loci)(aggr))
            yield Bed(chrom = k._1,
                      chromStart = k._2,
                      chromEnd = k._3,
                  ).overwriteMisc(v)
          }

        case (false, Some(grp)) =>
          for (s <- db.samples.eiterator)
          yield s.trackUpdated {
            for((k, v) <- s.track.partitionby(loci)(keep);
                (g, a) <- v.groupby(grp._2)(aggr)) 
            yield Bed(chrom = k._1,
                      chromStart = k._2,
                      chromEnd = k._3
                  ).addMisc(grp._1, g).overwriteMisc(a)
          }
      }
    }


/*
 * Omit for now
 *
    def groupbyS[G, A](grp: Sample => G)(a: SGroupByProj[A]*)(db: DB)
    : scala.collection.mutable.Map[G, Map[String, A]] = {
      //
      // I prefer groupby that retains a naturally nested grouping
      // structure. I also prefer groupby that does not assume
      // aggregate functions return Double. So here goes ...

      val bs = a
               .to(Vector)
               .map { case SGroupByProj(k, f) => f.sa |>((x: A) => (k, x)) }
      def mix(bs: Array[(String, A)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(bs:_*)(mix)
 //     if (a.to(Vector).isEmpty) db.samples.groupby(grp)(keep) else
      db.samples.groupby(grp)(b)
    }


    def groupbyR[R, B](grp: Bed => R)(a: BGroupByProj[B]*)(db: DB)
    : Iterator[(Sample, scala.collection.mutable.Map[R, Map[String, B]])] = { 
      // This is Synchrony's generalized groupby on track.
      // It follows the natural nested structure of groups.
      // A user can later process each resulting group in
      // whatever ways he like, w/o forcing the groups into
      // Bed/track right now.

      val bs = a
               .to(Vector)
               .map { case SProjBAggr(k, f)=>f.a |> ((x: B) => (k,x)) }
      def mix(bs: Array[(String, B)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(bs:_*)(mix)
      for(s <- db.samples.eiterator) 
      yield 
//      if (a.to(Vector).isEmpty) (s, s.track.groupby(grp)(keep)) else
        (s, s.track.groupby(grp)(b))
    }


    def partitionbyR[R, B](grp: Bed => R)(a: BGroupByProj[B]*)(db: DB)
    : Iterator[(Sample, Iterator[(R, Map[String, B])])] = {
      //
      // Similar to groupbyR, but assumes that the entries in
      // each group are sorted in a way consistent with "isBefore".
      val bs= a
              .to(Vector)
              .map { case SProjBAggr(k, f) => f.a |> ((x: B) => (k, x)) }
      def mix(bs: Array[(String, B)]) = Map(bs.toIndexedSeq:_*)
      val b = combineN(bs:_*)(mix)
      for(s <- db.samples.eiterator) 
      yield
//      if (a.to(Vector).isEmpty) (s, s.track.partitionby(grp)(keep)) else
        (s, s.track.partitionby(grp)(b))
    }

 *
 * End omission.
 */



  /** Emulation of GMQL's map.
   */

    def map
      (region: OnRegion[BGroupByProj[Double]] = OnRegion(), 
       joinby: OnSample[CanJoinS] = OnSample())
      (rdb: DB, edb: DB): 
    DB = { 

      val conflict = region.p.toVector.exists { case SProjBAggr(k, a) =>
        k == "chrom" || k == "chromStart" || k == "chromEnd" ||
        k == "name"  || k == "score"      || k == "strand"
      }

      def overwriteMisc(r: Bed, m: Map[String, Any]) = 
        if (conflict) r.overwriteMisc(m)
        else r.simpleOverwriteMisc(m)

      val b = mkAggrFromBGroupByProj(
        SProjBAggr("count", Count) +: region.p.to(Vector)
      )

      val nil = Vector[Bed]().flatAggregateBy(b) 

      def mkname(s: Sample, d: String) = 
        d + (if (s.hasM("name")) s.getM("name") else "")


      def parallelmap(ref: Sample, matches: Vector[Sample]) = {

        import synchrony.iterators.FileCollections.EFile.merge     
        import synchrony.iterators.Serializers.IncrementalSerializer
        import synchrony.genomeannot.BedWrapper.BedSerializers.BedFormatter     
        import java.nio.file.Files

        var id = 0.0

        val mm = for (m <- matches;
                      f = Files
                          .createTempFile(
                             ref.bedFile.efile.settings.prefix,
                             ref.bedFile.efile.settings.suffixtmp)
                          .toString;
                      w = IncrementalSerializer[Bed](f, BedFormatter)) 
                 yield { id = id + 1; (id, f, w, m) }

        for ((id, f, w, m) <- mm) { w.open() }

        val bb = for ((id, f, w, m) <- mm;
                      tr = for (r <- m.track) yield r.addMisc("$id", id)) 
                 yield BedFile.transientBedFile(tr)

        val merged = merge(bb :_*) 

        val lm = ref.bedFile.lmTrack

        val ex = merged.exTrack.sync(
                   lm       = lm,
                   isBefore = Bed.isBefore _,
                   canSee   = GenomeLocus.cond(GenomeLocus.Overlap(1)) _,
                   filter   = Bed.cond(),
                   isExclusive = false)

        for (r <- lm; 
             ii = ex syncedWith r;
             ee = ii.groupby(e => e.getMisc[Double]("$id"))(b);
             (id, f, w, m) <- mm;
             newr = if (ee contains id) overwriteMisc(r, ee(id).toMap)
                    else overwriteMisc(r, nil))
        {
          w.append(newr)         
        }          
           
        for ((id, f, w, m) <- mm) { w.close() }
        
        lm.close()

        val refN = ref.mRenamed(mkname(ref, "ref.") + _);
        for ((id, f, w, m) <- mm;
             bed = BedFile.onDiskBedFile(f, m.bedFile.efile.settings, true);
             eN = m.mRenamed(mkname(m, "expt,") + _))
        yield refN.mOverwritten(eN.meta).bedFileUpdated(bed)
      }


      // I am not sure why sequential map runs very slowly
      // when matches contains two or more samples. Sigh...
      //
      def sequentialmap(ref: Sample, matches: Vector[Sample]) = {
  
        val refN = ref.mRenamed(mkname(ref, "ref.") + _)

        for (e <- matches) 
        yield {
          val eN = e.mRenamed(mkname(e, "expt.") + _)
          val lm = refN.bedFile.lmTrack
          val ex = lm.sync(
                    ex       = eN.bedFile.exTrack,
                    isBefore = Bed.isBefore _,
                    canSee   = GenomeLocus.cond(GenomeLocus.Overlap(1)) _,
                    filter   = Bed.cond(),
                    isExclusive = false)
          val tr = for (r <- lm;
                       i = ex syncedWith r;
                       m = i flatAggregateBy b) 
                   yield overwriteMisc(r, m)
          val bed = BedFile.transientBedFile(tr).stored
          refN.mOverwritten(eN.meta).bedFileUpdated(bed)
        }
      }


      def switchmap(ref: Sample, matches: Iterator[Sample]) = {
        val mm = matches.toVector
        (mm.size < SWITCHTHRESHOLD) match {
          case true  => 
            if (DEBUG) println(s"** Sequential map...")
            sequentialmap(ref, mm)
          case false => 
            if (DEBUG) println(s"** Parallel map...")
            parallelmap(ref, mm)
        }
      }


      DB {
        for ((ref, matches) <- mapS(joinby.p:_*)(rdb, edb);
             sample <- switchmap(ref, matches))
        yield sample
      }
    }




    private def mapS(joinby: CanJoinS*)(rdb: DB, edb: DB) = {
      
      // GMQL has an unnecessarily restricted join predicate,
      // basically canJoinS(p1, p2, ...) where p1, p2, ...
      // are meta attributes, meaning two samples can join
      // if they agree on these meta attributes.
      //
      // Here, I allow more general join predicates,
      // viz. any Boolean function on a pair of samples.

      def canJoin(ref: Sample, e: Sample) = joinby.forall(p=> p(ref, e))

      (for (ref <- rdb.samples.eiterator;
            e <- edb.samples.eiterator; 
            if (canJoin(ref,e)))
       yield (ref, e)
      ).partitionby(_._1)(OpG.map(_._2))
    }

 

  /** Emulaton of GMQL's difference.
   * 
   *  This implementation assumes all the Sample tracks are
   *  sorted. 
   */

    def difference
      (joinby: OnSample[CanJoinS] = OnSample(), 
       exact:  Boolean            = false)
      (rdb: DB, edb: DB): 
    DB = {

      def overlap(x: Bed, y: Bed)= 
        if (exact) {x sameLocus y}
        else x.overlap(1)(y)

      def minus(ref: Sample, diffs: Iterator[Sample]) = diffs.hasNext match {

        case false => ref

        case true =>
          import synchrony.iterators.FileCollections.EFile.merge     
          val merged = merge(diffs.map(_.bedFile).toVector :_*)     
          val lm = ref.bedFile.lmTrack
          val ex = merged.exTrack.syncN(
                      lm       = lm,
                      isBefore = Bed.isBefore _,
                      canSee   = GenomeLocus.cond(GenomeLocus.Overlap(1)) _,
                      filter   = Bed.cond(),
                      n        = 1,
                      isExclusive = false)
          val tr = lm.filter(r => ex.syncedWith(r).isEmpty)
          val bed = BedFile.transientBedFile(tr).stored
          ref.bedFileUpdated(bed)
      }
    
      DB {
        for ((ref, diffs) <- mapS(joinby.p :_*)(rdb, edb))
        yield minus(ref, diffs)
      }
    }



    //
    // Auxiliary functions for emulating GMQl's syntax
    // for project, extend, and groupby.

    private def split(ps: Vector[SProj], x: Vector[SProj], y: Vector[SProj]):
    (Vector[SProj], Vector[SProj]) = {

      // Split a list of projections into aggregate and non-aggregate functions.

      if (ps.isEmpty) (x, y) else ps.head match {
        case p: SPObj[_]       => split(ps.tail, x :+ p, y)
        case p: SProjSimple[_] => split(ps.tail, x :+ p, y)
        case p: SProjGen[_]    => split(ps.tail, x :+ p, y)
        case p: SProjBAggr[_]  => split(ps.tail, x,      y :+ p)
        case p: SProjSBAggr[_] => split(ps.tail, x,      y :+ p)
      }
    }


    private def mkAggrFromSProj(ps: Vector[SProj])(s:Sample) =  {

      // Combine multiple independent aggregate functions
      // on regions/track into a single aggregate function, 
      // so that the input track is scanned only once.
      // NOTE: As required by GMQL, the aggregate functions
      // are assumed to produce Double as results.
  
      def cvt[A](a: Aggr[Bed, A]) = a.asInstanceOf[Aggr[Bed, Double]]
      val b = ps.map {
        case SProjSBAggr(k, sa) => cvt(sa.sa(s)) |> ((x: Double) => (k, x))
        case SProjBAggr(k,a)    => cvt(a.a) |> ((x: Double) => (k, x))
      }
      def mix(b: Array[(String, Double)]) = Map(b.toIndexedSeq:_*)
      combineN(b:_*)(mix)
    }

    

    private def mkAggrFromBGroupByProj(ps: Vector[BGroupByProj[Double]]) = { 

      def cvt[A](a: Aggr[Bed, A]) = a.asInstanceOf[Aggr[Bed, Double]]
      val b = ps.map { 
        case SProjBAggr(k, a) => cvt(a.a) |> ((x: Double) => (k, x))
      }
      def mix(b: Array[(String, Double)]) = Map(b.toIndexedSeq:_*)
      combineN(b:_*)(mix)
    }
 

    private def mkAggrFromSGroupByProj(as: Vector[SGroupByProj[Double]]) = { 

      val bs= as.to(Vector).map {
        case SGroupByProj(k, f) => f.sa.|>((x: Double) => (k, x))
      }
      def mix(bs: Array[(String, Double)]) = Map(bs.toIndexedSeq:_*)
      combineN(bs:_*)(mix)
    }



  /** Emulation of GMQL's join
   */

    private def splitGenoPred(
      ps:  Vector[LocusPred],
      ycs: Vector[LocusPred],
      ncs: Vector[LocusPred]
    ): (Vector[LocusPred], Vector[LocusPred]) = {
      //
      // split ps into those (ycs) convex wrt isBefore
      // and those which are not (ncs).

      if (ps.isEmpty) (ycs, ncs) else ps.head match {
        case p:GenomeLocus.DLE     => splitGenoPred(ps.tail, ycs :+ p, ncs)
        case p:GenomeLocus.DL      => splitGenoPred(ps.tail, ycs :+ p, ncs)
        case p:GenomeLocus.Overlap => splitGenoPred(ps.tail, ycs :+ p, ncs)
        case p         => splitGenoPred(ps.tail, ycs,      ncs :+ p)
      }
    }



    def join
      (limit:   Int                = 1000,
       pred:    Genometric         = Genometric(), 
       joinbyS: OnSample[CanJoinS] = OnSample(),
       joinbyR: OnRegion[CanJoinR] = OnRegion(),
       outputS: OutputS            = OverwriteS(),
       outputR: OutputR            = BothR(),
       orderR:  OrderR             = DoneR)
      (rdb: DB, edb: DB): 
    DB = {

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
      // in Synchrony via sliding-windows or via joinNearest.
      //
      // NOTE: GMQL puts unnecessary restricted on join
      // predicate for regions, basically 
      // canJoinR(p1, p2, ...) where p1, p2, ... are meta
      // attributes of regions. I.e., two regions can
      // join if they agree on these attributes. In Synchrony,
      // we allow more general join predicates, viz. any
      // Boolean function on a pair of regions.

      def canJoinR(r: Bed, t: Bed) = joinbyR.p.forall(f => f(r, t))

      val (ycs, ncs) = splitGenoPred(
        pred.g.to(Vector),
        Vector(GenomeLocus.DLE(limit)),
        Vector()
      )


      // The "paralleljoin" of anchor with several expts is based
      // on the following strategy, assuming both anchor and expts
      // are already sorted:
      // - tag each bed entry in each expt with an expt id.
      // - merge the expts into a single stream in sorted order.
      // - run sychrony iterator on the anchor and this merged stream.
      // - on each step of the synchrony iteration, do a groupby on
      //   the expt id of the syncedWith result of this step.
      // - write each group to their separate files.
      // - at the end of the synchrony iteration, put these separate
      //   into the right pair of join samples.
      //
      // This strategy is fast because merging is cheap when data is
      // already sorted, and synchrony iteration is cheap on two
      // sorted streams. The main saving is that the anchor file is
      // read only once. The main overhead paid is in writing n files
      // currently; this will be slower on most systems than writing
      // a single big file. 

      def paralleljoin(anchor: Sample, expts: Vector[Sample]) = {

        import synchrony.iterators.FileCollections.EFile.merge     
        import synchrony.iterators.Serializers.IncrementalSerializer
        import synchrony.genomeannot.BedWrapper.BedSerializers.BedFormatter     
        import java.nio.file.Files

        var id = 0.0

        val mm = for (m <- expts;
                      f = Files
                          .createTempFile(
                             anchor.bedFile.efile.settings.prefix,
                             anchor.bedFile.efile.settings.suffixtmp)
                          .toString;
                      w = IncrementalSerializer[Bed](f, BedFormatter)) 
                 yield { id = id + 1; (id, f, w, m) }

        for ((id, f, w, m) <- mm) { w.open() }

        val bb = for ((id, f, w, m) <- mm;
                      tr = for (r <- m.track) yield r.addMisc("$id", id)) 
                 yield BedFile.transientBedFile(tr)

        val merged = merge(bb :_*) 

        val lm = anchor.bedFile.lmTrack

        val ex = merged.exTrack.sync(
                   lm       = lm,
                   isBefore = Bed.isBefore _,
                   canSee   = GenomeLocus.cond(ycs:_*),
                   filter   = Bed.cond(),
                   isExclusive = false)

        def grp(b: Bed) = b.getMisc[Double]("$id")
        def tidyup(b: Bed) = b.delMisc("$id")

        for (r <- lm;
             ii = (ex syncedWith r)
                  .filter { t => canJoinR(r, t) && Bed.cond(ncs: _*)(t, r) };
             ee = ii.groupby(grp _)(OpG.map(tidyup));
             (id, f, w, m) <- mm;
             tt <- ee.get(id);
             t <- tt;
             joined <- outputR(r,t))
        {
          w.append(joined)  
        }

        for ((id, f, w, m) <- mm) { w.close() }

        lm.close()

        for ((id, f, w, m) <- mm;
             bed = BedFile.onDiskBedFile(f, m.bedFile.efile.settings, true);
             orderedBed = orderR match {
                            case DoneR => bed
                            case SortR => bed
                            case DistinctR => DistinctR(bed)
                            case SortDistinctR => DistinctR(bed)
                          })
        yield outputS(anchor, m).bedFileUpdated(orderedBed)
      }


      // The sequentialjoin is a simple nested loops of synchrony
      // iterator. It reads the anchor file as many times as there
      // are expts. It is much faster than the paralleljoin when
      // there is one expt file. In theory, it is not much slower
      // until you have lots of expt files.
      //
      // But somehow it runs cery slowly when you have more than
      // one expt. I am not sure where the problem is yet...

      def sequentialjoin(anchor: Sample, expts: Vector[Sample]) = {
        for (e <- expts;
             lm = anchor.bedFile.lmTrack;
             ex = e.bedFile.exTrack.sync(
                     lm       = lm,
                     isBefore = Bed.isBefore _,
                     canSee   = GenomeLocus.cond(ycs:_*),
                     filter   = Bed.cond(),
                     isExclusive = false);
             tr = for(r <- lm;
                      t <- ex syncedWith r;
                      if (canJoinR(r, t));
                      if (Bed.cond(ncs:_*)(t,r));
                      joined <- outputR(r,t))
                  yield joined;
             orderedtr = orderR match {
                           case DoneR => tr
                           case SortR => tr
                           case DistinctR => DistinctR(tr)
                           case SortDistinctR => DistinctR(tr)
                         })
        yield outputS(anchor, e).trackUpdated(orderedtr)
      }


     def switchjoin(anchor: Sample, expts: Iterator[Sample]) = {
        val mm = expts.toVector
        (mm.size < SWITCHTHRESHOLD) match {
          case true  => 
            if (DEBUG) println(s"** Sequential join...")
            sequentialjoin(anchor, mm)
          case false => 
            if (DEBUG) println(s"** Parallel join...")
            paralleljoin(anchor, mm)
        }
      }


      DB {
        for ((anchor, expts) <- mapS(joinbyS.p:_*)(rdb, edb);
             sample <- switchjoin(anchor, expts))
        yield sample
      }
    }


      

  /** A restricted emulation of GMQL's MD(1) genometric queries.
   *
   *  TODO: joinNearest is not very efficiently implemented.
   */

    def joinNearest
      (limit: Int                  = 200000,
       up: Int                     = 1,
       down: Int                   = 1,
       inclMid: Boolean            = false,
       pred: Genometric            = Genometric(), 
       joinbyS: OnSample[CanJoinS] = OnSample(),
       joinbyR: OnRegion[CanJoinR] = OnRegion(),
       outputS: OutputS            = OverwriteS(),
       outputR: OutputR            = BothR(),
       orderR: OrderR              = DoneR)
      (rdb: DB, edb: DB)
    : DB = {

      Step0 {
        mapS(joinbyS.p:_*)(rdb, edb)
      }
     .Step1 { db =>

        def nearest(r: Bed, ts: Vector[Bed]) = {
          def grp(x: Bed) = (x endBefore r, x distFrom r)
          val groups = (ts.partitionby(grp)(keep)).toVector
          val (u, md) = groups.span(_._1._1)
          val (m, d) = md.span(_._1._2 == 0)
          def get(n: Int, groups: Vector[(Int, Vector[Bed])]) = 
            for (ts <- groups.sortBy(_._1).take(n); t <- ts._2) yield t
          def upstr = get(up, for(((p, d), v) <- u) yield (d, v))
          def downstr = get(down, for(((p, d), v) <- d) yield (d, v))
          def midstr = for(((p, d), v) <- m; t <- v) yield t
          upstr ++ { if (inclMid) midstr else Vector() } ++ downstr
        }

        def canJoinR(r: Bed, t: Bed) = joinbyR.p.forall(f => f(r, t))

        val (ycs, ncs) = splitGenoPred(
          pred.g.to(Vector), 
          Vector(GenomeLocus.DLE(limit)),
          Vector()
        )

        DB {
          for((anchor, expts) <- db;
              e <- expts;
              lm = anchor.bedFile.lmTrack;
              ex = e.bedFile.exTrack.sync(
                lm       = lm,
                isBefore = Bed.isBefore _,
                canSee   = GenomeLocus.cond(ycs:_*),
                filter   = Bed.cond(),
                isExclusive = false);
              tr = for(r <- lm;
                       ts = ex syncedWith r;
                       t <- nearest(r, ts);
                       if (canJoinR(r, t));
                       if (Bed.cond(ncs:_*)(t,r));
                       joined <- outputR(r,t))
                   yield joined)
          yield outputS(anchor, e).trackUpdated(orderR(tr)) 
        }
      }
     .Done
    }



  /** ere are some auxiliary functions to emulate syntax for
   * GMQL's more restricted/specialised join predicates and 
   * output formats.
   */

    
    abstract class OutputS {
      def apply(x: Sample, y: Sample): Sample
    }

    case object LeftS extends OutputS {
      def apply(x: Sample, y: Sample) = x
    }

    case object RightS extends OutputS {
      def apply(x: Sample, y: Sample) = y
    }

    case class OverwriteS(f: String => String = (p => "r."++p))
    extends OutputS {
      def apply(x: Sample, y: Sample) = x mOverwritten y.mRenamed(f).meta
    }
 
    case class GenS(f: (Sample, Sample) => Sample) extends OutputS {
      def apply(x: Sample, y: Sample) = f(x, y)
    }
    

    abstract class OutputR {
      def apply(x: Bed, y: Bed): Option[Bed]
    }

    case object LeftR extends OutputR {
      def apply(x: Bed, y: Bed) = Some(x)
    }

    case object RightR extends OutputR {
      def apply(x: Bed, y: Bed) = Some(y)
    }

    case class IntR(f: String => String = (p => "r."++p))
    extends OutputR {
      def apply(x: Bed, y: Bed) = (x.locus intersect y.locus) match {
        case None => None
        case Some(locus) => Some(Bed(locus).
                                 overwriteMisc(x.misc).
                                 overwriteMisc(y.renameMisc(f).misc))}
    }

    case class BothR(f: String => String = (p => "r."++p))
    extends OutputR {
      def apply(x: Bed, y: Bed) = Some(x.overwriteMisc(Map(
        f("chrom") -> y.chrom,
        f("chromStart") -> y.chromStart,
        f("chromEnd") -> y.chromEnd)))
    }

    case class CatR(f: String => String = (p => "r."++p))
    extends OutputR {
      def apply(x: Bed, y: Bed) = (x.locus union y.locus) match {
        case None => None
        case Some(locus) => Some(Bed(locus)
                                 .overwriteMisc(x.misc)
                                 .overwriteMisc(y.renameMisc(f).misc))
      }
    }

    case class GenR(f: (Bed, Bed) => Option[Bed]) extends OutputR {
      def apply(x: Bed, y: Bed) = f(x, y)
    }

   
    abstract class OrderR {
      def apply(it: Iterator[Bed]): Iterator[Bed]
      def apply(bf: BedFile): BedFile
    }

    case object DoneR extends OrderR {
      def apply(it: Iterator[Bed]) = it
      def apply(bf: BedFile) = bf
    }

    case object DistinctR extends OrderR {
      def apply(it: Iterator[Bed]) = {
        var tmp: Option[Bed] = None
        for(b <- it;
            if (tmp match {
              case None => { tmp = Some(b); true}
              case Some(x) => if (b sameLocus x) false
                              else { tmp = Some(b); true }
            }))
        yield b
      }

      def apply(bf: BedFile) = BedFile.transientBedFile(apply(bf.eiterator))
    }

    case object SortR extends OrderR {
      def apply(it: Iterator[Bed]) = it.to(Vector).sorted.iterator
      def apply(bf: BedFile) = bf.sorted
    }

    case object SortDistinctR extends OrderR {
      def apply(it: Iterator[Bed]) = DistinctR(SortR(it))
      def apply(bf: BedFile) = DistinctR(SortR(bf))
    }

  } // End object DB



  object implicits {

    import scala.language.implicitConversions

    implicit def SampleFile2DB(samples: SampleFile): DB = DB(samples)

    implicit def DB2SampleFiles(db: DB): SampleFile = db.samples

    implicit def DB2SampleEIterator(db: DB): SampleEIterator =
      db.samples.eiterator

  }

}  // End object GMQL




