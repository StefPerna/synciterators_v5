
package gmql

/** Version 5, to be used with gmql.BEDModel and gmql.SAMPLEModel version 5.
 *
 *  Wong Limsoon
 *  10 February 2022
 */



object SAMPLEFileOps {

/** Synchrony GMQL
 *
 *  This package provides operations on samples and their associated 
 *  BED files. It is an emulation of GMQL, ignoring some of its syntax
 *  idiosyncrasies, while improving the orthogonality of its design
 *  and efficiency. This implementation is about 10x more compact
 *  than the original GMQL. And, due to use of Synchrony iterator,
 *  it is also more efficient than the original GMQL.
 *
 *  Conceptually, the data manipulated can be thought of as a simple
 *  nested relation, 
 *
 *     SampleFile[Sample(l1, .., 
 *                       ln, 
 *                       bedFile: BedFile[Bed(chrom, start, end,
 *                                            name, score, strand,
 *                                            r1, ..., rm)] )]
 *
 *  where l1, .., ln are metadata attributes of a sample; r1, .., rm
 *  are metadata attributes of a genomic region (a row in a BED file).
 *  A SampleFile can be thought of as a "file-based vector", which
 *  transparently materializes the parts of the vector of samples and
   BED rows into memory when they are needed in a computation.
 *
 *  The main query operations provided by GMQL are mirrored after the
 *  relational algebra (SELECT, PROJECT, JOIN, GROUPBY) on samples,
 *  ignoring the bedFile attribute; aggregate functions on the bedFile
 *  attribute; and if we regard the bedFile attribute as a relational
 *  table, GMQL provides also relational algebra operations (SELECT,
 *  PROJECT, JOIN, GROUPBY) on it, as well as operations with 
 *  specialized genomic meaning (MAP, DIFFERENCE, and COVER) on it.
 *
 *  MAP is equivalent to a JOIN+GROUPBY+aggregation function query: it
 *  compares two BED files (call these the landmark and experiment
 *  tracks), grouping the regions/BED entries in the experiment track
 *  to regions/BED entries that their loci overlap with in the landmark
 *  track, applying some aggregate functions on each resulting group,
 *  and finally producing for each region in the landmark track, the
 *  corresponding aggregate function results as the output.
 *
 *  DIFFERENCE is similar to MAP, but the landmark track is compared 
 *  to multiple experiment tracks. The output is those regions in the
 *  landmark track that t overlap with no region in all the experiment
 *  tracks.
 *
 *  The JOIN on (two) tracks supports the so-called genometric
 *  predicates, which are essentially "greater/less than" predicates
 *  on the distance between two regions in the two tracks. It also
 *  supports a special genometric predicate, MD(k), which tests whether
 *  a region/BED entry in one track is a k-nearest neighbour of another
 *  region/BED entry in another track in terms of genomic distance.
 *
 *  COVER is more complicated, and there is no obvious efficient
 *  relational query equivalent. In its simplest form COVER(n,m),
 *  applied to a set of tracks, outputs (sub)regions that overlap with
 *  at least n and atmost m of the tracks.
 *
 *  These operations on tracks/bedFiles are implemented in the module
 *  BEDFileOps. The operations on samples are implemented in this
 *  module.
 */


  import scala.language.implicitConversions
  import dbmodel.DBModel.CloseableIterator.CBI
  import dbmodel.DBModel.OrderedCollection.OSeq
  import dbmodel.DBModel.OpG
  import dbmodel.DBModel.OpG.AGGR
  import dbmodel.DBFile.OFile
  import gmql.SAMPLEModel.SAMPLE._
  import gmql.SAMPLEModel.SAMPLEFile._
  import gmql.BEDModel.BEDEntry.Bed
  import gmql.BEDFileOps._

  type SAMPLE     = gmql.SAMPLEModel.SAMPLE.Sample
  type SAMPLEFILE = gmql.SAMPLEModel.SAMPLEFile.SAMPLEFILE
  type BED        = gmql.BEDModel.BEDEntry.Bed
  type BEDFILE    = gmql.BEDModel.BEDFile.BEDFILE
  type META       = Map[String,Any]
  type Bool       = Boolean
  type SAMPLEPRED = (Sample,Sample)   => Bool
  type SAMPLEOP   = (Sample,Sample)   => Sample
  type BEDFILEOP  = (BEDFILE,BEDFILE) => BEDFILE

  val SampleFile  = gmql.SAMPLEModel.SAMPLEFile.SampleFile
  val BedFile     = gmql.BEDModel.BEDFile.BedFile


  var ANS: String = "."   // Default folder for result files
  var STORE = false       // If true, always serialize result files




  object QUERYEngines
  {
    /** This module provides both a sequential GMQL query engine and
     *  a sample-parallel GMQL query engine. [[QueryEngine]] encapsulates
     *  the sequential mode.  [[ParallelQueryEngine]] encapsulates the
     *  sample parallel mode.  [[GMQLEngine]] is the common GMQL operations
     *  to be defined on top of either mode.
     */

    trait QueryEngine
    {
      /** the Sample file to be queried
       */

      def samples: SAMPLEFILE

      /** Combinators for composing queries.
       */
    
      def mapSample(f: SAMPLE => SAMPLE): SAMPLEFILE =
        samples.map(f).materialized


      def mapRegion(f: BEDFILE => BEDFILE): SAMPLEFILE =
        samples.map { s => s.newTrack(f(s.bedFile)) } .materialized


      def filterSample(f: SAMPLE => Bool): SAMPLEFILE =
        samples.filter(f).materialized


      def joinby
        (otherSamples: SAMPLEFILE, preds: SAMPLEPRED*)
        (f: (Sample, IterableOnce[Sample])=>IterableOnce[Sample]): SAMPLEFILE =
      {
        def check(s: Sample, t: Sample) = preds.forall { _(s, t) }
        val others: SAMPLEFILE = otherSamples.materialized
        samples.flatMap { s => f(s, others.elemSeq.filter(check(s, _))) }
               .materialized.userev(others)
      }

 
      def joinbyCP
        (otherSamples: SAMPLEFILE, preds: SAMPLEPRED*)
        (f: (Sample, Sample) => Sample): SAMPLEFILE =
      {
        def check(s: Sample, t: Sample) = preds.forall { _(s, t) }
        val others: SAMPLEFILE = otherSamples.materialized
        samples.flatMap { s =>
          others.elemSeq.filter { check(s, _) } .map { t => f(s, t) }
        }.materialized.userev(others)
      }


      def groupby
        (groupby: String*)
        (f: (META, Seq[Sample]) => IterableOnce[Sample]): SAMPLEFILE =
      {
        def grp(s: Sample) = Map(groupby map { g => g -> s(g) }: _*)
        SampleFile.transientSampleFile {
          samples.elemSeq.groupBy(grp _)
                 .toSeq.flatMap { case (ms,ts) => f(ms,ts) }
        }
      }

    } // End trait QueryEngine



    trait GMQLEngine
    { 
      SELF: QueryEngine =>

      /** Serialize after sorting tracks in the result.
       */

      def tracksSorted(
        filename: String = "", 
        folder: String = ANS): SAMPLEFILE = 
      {
        samples.map(_.orderedTrack).serialized(filename, folder)
      }


      /** Save the results using a new filename & folder. Make fresh copies
       *  of associated Bed files.
       */
 
      def deepSaveAs(filename: String, folder: String = ANS): SAMPLEFILE = 
        SampleFile.deepCopy(samples, filename, folder)(deep = true)


      /** GMQL queries 
       */

      def store(sf: SAMPLEFILE): SAMPLEFILE =
        { if (STORE) sf.serialized else sf }.userev(samples)


      /** [[onRegion(f)]] applies a query [[f]] on every track in samples.
       */

      def onRegion(f: BEDFILE => BEDFILE): SAMPLEFILE = store { mapRegion(f) }


      /** [[selectS(f)]]
       *
       *  SELECT s.* FROM samples s WHERE f(s)
       */

      def selectS(onSample: Sample => Bool): SAMPLEFILE =
        store { filterSample(onSample) }


      /** [[projectS(l1 -> f1, .., ln -> fn)]]
       *
       *  SELECT l1 as f1(s), .., ln as fn(s) FROM samples s
       */

      def projectS(onSample: (String, Sample=>Any)*): SAMPLEFILE = store {
        mapSample { s =>
          s.newMeta { Map(onSample map { case (k, f) => (k -> f(s)) }: _*) }
        }
      }


      /** [[extendS(l1 -> f1, .., ln -> fn)]]
       *
       *  SELECT s.*, l1 as f1(s), .., ln as fn(s) FROM samples s
       */

      def extendS(onSample: (String, Sample=>Any)*): SAMPLEFILE = store { 
        mapSample { s => 
          s ++ (onSample map { case (k, f) => (k -> f(s)) }: _*)
        }
      }


      /** [[groupbyS(groupby, l1 -> a1, .., ln -> an)]]
       *
       *  SELECT group as s.g, l1 as A1, .., ln as An
       *  FROM samples s
       *  GROUPBY s.g
       *  WITH A1 = results of aggregate function a1 on a group, ..,
       *       An = results of aggregate function an on a group
       */

      def groupbyS(
        groupby: String,
        aggrs: (String, AGGR[Sample,Any])*): SAMPLEFILE =
      {
        val aggr = OpG.combine(aggrs: _*)
        store { this.groupby(groupby) { 
          (ms: META, ts: Seq[Sample]) => 
                  ts.map { t => t ++ (ms ++ aggr(ts)) }
          }
        }
      }


      /** [[differenceS(that, joinby)(exact)]]
       *
       *  SELECT s.*, 
       *         differenceR(exact)(
       *           s.BedFile,
       *           SELECT t.BedFile FROM that t WHERE joinby(s, t))
       *  FROM samples s
       *
       *  differenceR(exact)(us, d1, .., dn) is this pseudo SQL query on
       *  us:BedFile, d1:BedFile, ...dn:BedFile,
       *
       *  SELECT u.*
       *  FROM us u
       *  WHERE exact && locus of u is not in any of d1, .., dn
       *     OR (!exact) && locus of u doesnt overlap any region in d1, .., dn.
       */

      def differenceS
        (otherSamples: SAMPLEFILE, 
         joinby: SAMPLEPRED*)
        (implicit exact: Bool = false): SAMPLEFILE =
      {
        store { this.joinby(otherSamples, joinby: _*) { 
          (s: Sample, ts: IterableOnce[Sample]) =>
            val bs = ts.iterator.toSeq.map(_.bedFile)
            val nb = if (bs.isEmpty) s.bedFile
                     else s.bedFile.differenceR(bs: _*)(exact)
            Some(s.newTrack(nb))
          }
        }.userev(otherSamples) 
      }
    

      /** [[joinS(that, joinby)(f, g)]]
       *
       *  SELECT g(s, t).bedFileUpdated(f(s.BedFile, t.BedFile))
       *  FROM samples s, that t
       *  WHERE joinby(s,t)
       */

      def joinS
        (otherSamples: SAMPLEFILE, 
         joinby: SAMPLEPRED*)
        (implicit
           onRegion: BEDFILEOP = BFOps.joinR(Genometric.OVERLAP(1).apply _),
           onSample: SAMPLEOP  = SFOut.OVERWRITE): SAMPLEFILE =
      {
// This version is not very parallelizable.
//        this.joinby(otherSamples, joinby: _*) { 
//          (s: Sample, ts: IterableOnce[Sample]) => 
//             CBI(ts.iterator).map { t => 
//               onSample(s, t) newTrack onRegion(s.bedFile, t.bedFile)
//          }
//        }
        store { this.joinbyCP(otherSamples, joinby :_*) {
          (s: Sample, t: Sample) => 
             onSample(s, t) newTrack onRegion(s.bedFile, t.bedFile)
          }
        }.userev(otherSamples)
      }


      /** [[mapS(that, joinby)(f, g)]]
       *
       *  SELECT g(s, t).newTrack(f(s.BedFile, t.BedFile))
       *  FROM samples s, that t
       *  WHERE joinby(s,t)
       *
       *  It is actually same as [[joinS]]. However, [[f]] is given a
       *  default function that corresponds to [[BFOps.mapR]]. I.e. [[f]] 
       *  is by default the following query on [[s.bedFile]]:
       *
       *  SELECT u.* count as COUNT
       *  FROM s u, t v
       *  WHERE u.loc overlaps v.loc
       *  GROUPBY u.loc                   // locus of u 
       *  WITH COUNT = size of a group
       */

      def mapS
        (otherSamples: SAMPLEFILE, 
         joinby: SAMPLEPRED*)
        (implicit
           onRegion: BEDFILEOP = BFOps.mapR()("count"),
           onSample: SAMPLEOP  = SFOut.LEFT): SAMPLEFILE =
      {
        joinS(otherSamples, joinby: _*)(onRegion, onSample)
      }
     

      /** [[coverS(onRegion, groupby)]] groups samples by metadata [[g]],
       *  then applies the function [[onRegion]] on the tracks of each group.
       *  Usually, onRegion is the [[BFOps.coverR]].
       *
       *  GMQL: COVER(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
       *  In Synchrony, this is expressed as:
       *  {{{
       *     import BFOps.coverR, BFCover.BETWEEN, BFAggr.SMALLEST
       *     db.coverS(
       *       onRegion = coverR(BETWEEN(2,3), "minPVal" -> SMALLEST("pval")),
       *       groupby  = "cell")
       *  }}}
       *
       *  GMQL: FLAT(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
       *  In Synchrony, this is expressed as:
       *  {{{
       *     import BFOps.flatR, BFCover.BETWEEN, BFAggr.SMALLEST 
       *     db.coverS(
       *       onRegion = flatR(BETWEEN(2,3), "minPVal" -> SMALLEST("pval")),
       *       groupby  = "cell")
       *  }}}
       *
       *  GMQL: SUMMIT(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
       *  In Synchrony, this is expressed as:
       *  {{{
       *     import BFOps.summitR, BFCover.BETWEEN, BFAggr.SMALLEST 
       *     db.coverS(
       *       onRegion = summitR(BETWEEN(2,3), "minPVal" -> SMALLEST("pval")),
       *       groupby  = "cell")
       *  }}}
       *
       *  GMQL: HISTOGRAM(2,3; groupby:cell; aggregate: minPVal as MIN(pval)) db
       *  In Synchrony, this is expressed as:
       *  {{{
       *     import BFOps.histoR, BFCover.BETWEEN, BFAggr.SMALLEST 
       *     db.coverS(
       *       onRegion = histoR(BETWEEN(2,3), "minPVal" -> SMALLEST("pval")),
       *       groupby  = "cell")
       *  }}}
       */

      def coverS(
        onRegion: Seq[BEDFILE]=>BEDFILE, 
        groupby: String*): SAMPLEFILE =
      {
        store { this.groupby(groupby: _*) { 
          (ms: META, ts: Seq[Sample]) =>
             Some(Sample(ms, onRegion(ts.map(_.bedFile).toSeq)))
          }
        }
      }

    }  // End trait GMQLengine



    case class GMQLEngineCompanion(cvt: SAMPLEFILE => GMQLEngine)
    {
      /** GMQL query operators, in function form.
       */

      def onRegion(f: BEDFILE => BEDFILE): SAMPLEFILE => SAMPLEFILE =
        samples => cvt(samples).onRegion(f)
 

      def selectS(onSample: Sample => Boolean): SAMPLEFILE => SAMPLEFILE =
        samples => cvt(samples).selectS(onSample)


      def projectS(onSample: (String,Sample=>Any)*): SAMPLEFILE => SAMPLEFILE =
        samples => cvt(samples).projectS(onSample: _*)


      def extendS(onSample: (String,Sample=>Any)*): SAMPLEFILE => SAMPLEFILE =
        samples => cvt(samples).extendS(onSample: _*)


      def groupbyS(
        groupby: String, 
        aggrs: (String, AGGR[Sample, Any])*): SAMPLEFILE => SAMPLEFILE =
      {
        samples => cvt(samples).groupbyS(groupby, aggrs: _*)
      }


      def differenceS(
        exact:  Bool, 
        joinby: SAMPLEPRED*): (SAMPLEFILE,SAMPLEFILE) => SAMPLEFILE =
      {
        (samples, that) => cvt(samples).differenceS(that, joinby: _*)(exact)
      }


      def joinS
        (onRegion: BEDFILEOP,
         onSample: SAMPLEOP,
         joinby: SAMPLEPRED*): (SAMPLEFILE,SAMPLEFILE) => SAMPLEFILE =
      {
        (samples,that) => cvt(samples).joinS(that, joinby:_*)(onRegion,onSample)
      }
  
  
      def mapS
        (onRegion: BEDFILEOP,
         onSample: SAMPLEOP, 
         joinby:   SAMPLEPRED*): (SAMPLEFILE,SAMPLEFILE) => SAMPLEFILE =
      {
        (samples,that) => cvt(samples).mapS(that, joinby:_*)(onRegion,onSample)
      }


      def coverS
        (onRegion: Seq[BEDFILE] => BEDFILE,
         groupby: String*): SAMPLEFILE => SAMPLEFILE =
      {
        samples => cvt(samples).coverS(onRegion, groupby: _*)
      }

    } // End GMQLEngineCompanion


    /** Boiler-plate codes for concurrency management. Needed for
     *  implementing sample parallelism.
     */

    object ExecutionManagers
    {
      import java.util.concurrent.{ Executors, ExecutorService, ForkJoinPool }
      import scala.concurrent.duration.Duration
      import scala.concurrent.{ Future, Await, 
                                ExecutionContextExecutorService, 
                                ExecutionContext }


      class ExecutionMgr(mkPool: Int => ExecutorService)
      {
        private val concurrency = Runtime.getRuntime.availableProcessors()

        def runBig[A](codes: => CBI[() => A]): CBI[A] = { 
          val executors: ExecutorService = mkPool(concurrency)
          implicit val pool = ExecutionContext.fromExecutorService(executors)
          val todo = codes
          // 100 at-a-time.
          val groups: CBI[Seq[() => A]] = todo.doit { _.grouped(100) }
          val futures       = for (grp <- groups; 
                                   g   <- grp.map(a => Future{ a() }))
                              yield g
          def hasnx(): Bool = futures.hasNext
          def nx(): A       = Await.result(futures.next(), Duration.Inf)
          def cl(): Unit    = { pool.shutdownNow(); todo.close() }
          CBI[A](hasnx _, nx _, cl _)
        }

        def runVec[A](codes: => Seq[() => A]): Seq[A] = {
          val executors: ExecutorService = mkPool(concurrency)
          implicit val pool = ExecutionContext.fromExecutorService(executors)
          val futures = Future.sequence(codes.map(a => Future{ a() }))
          val result = Await.result(futures, Duration.Inf)
          pool.shutdownNow()
          result
        }
      }


      val ParGMQLMgr = {
        def mkCachedThreads(cc: Int) = Executors.newCachedThreadPool()
        new ExecutionMgr(mkCachedThreads _)
      }

    }  // End object ExecutionManagers


    /** Now can define sample-parallel QueryEngine
     */

    trait ParallelEngine extends QueryEngine 
    {
      import SampleFile.transientSampleFile
      import ExecutionManagers.ParGMQLMgr.{ runVec, runBig }

      private def lift[A,B](f: A => B) = (a: A) => () => f(a)


      override def mapSample(f: SAMPLE => SAMPLE): SAMPLEFILE =
        transientSampleFile { 
          runVec { samples.elemSeq.map(lift(f)) }
        }


      override def mapRegion(f: BEDFILE => BEDFILE): SAMPLEFILE =
        transientSampleFile {
          runVec { 
            samples.elemSeq.map { lift(s => s.newTrack(f(s.bedFile))) }
          }
        }


      override def joinby
        (otherSamples: SAMPLEFILE, preds: SAMPLEPRED*)
        (f: (Sample, IterableOnce[Sample])=>IterableOnce[Sample]): SAMPLEFILE =
      {
        def check(s: Sample, t: Sample) = preds.forall { _(s, t) }
        val others: SAMPLEFILE = otherSamples.serialized
        transientSampleFile {
          runBig {
            samples.map {
              lift(s => f(s, others.elemSeq.filter(check(s, _))))
            }
          }.flatMap { us => us }
        }.userev(others)
      }


      override def joinbyCP
        (otherSamples: SAMPLEFILE, preds: SAMPLEPRED*)
        (f: (Sample, Sample) => Sample): SAMPLEFILE =
      {
        def check(s: Sample, t: Sample) = preds.forall { _(s, t) }
        val others: SAMPLEFILE = otherSamples.serialized
        transientSampleFile {
          runBig {
            samples.flatMap { s =>
              others.elemSeq.filter { check(s, _) } 
                            .map { lift(t => f(s, t)) }
            }
          }
        }.userev(others)
      }

    }  // End trait ParallelEngine

  }  // End object QUERYEngines



  object SequentialGMQL
  {
    import QUERYEngines.{ QueryEngine, GMQLEngine, GMQLEngineCompanion }

    implicit class SFOps(override val samples: SAMPLEFILE)
    extends QueryEngine with GMQLEngine

    val SFOps = GMQLEngineCompanion(new SFOps(_))

  }  // End object SequentialGMQL



  object ParallelGMQL
  {
    import QUERYEngines.{ ParallelEngine, GMQLEngine, GMQLEngineCompanion }

    implicit class SFOps(override val samples: SAMPLEFILE)
    extends ParallelEngine with GMQLEngine

    val SFOps = GMQLEngineCompanion(new SFOps(_))
   
  }  // End object ParallelGMQL



  /** SFSemiJoin provides a semi-join for use with [[selectS]].
   */

  implicit class SFSemiJoin(sample: Sample) {
    def semijoin(in: String*)(ex: String*)(db: SAMPLEFILE): Bool =
      db.elemSeq.exists { s =>
        in.forall { l => s(l) == sample(l) } &&
        ex.forall { l => s(l) != sample(l) }
      }
  }


  /** SFOut provides the common ways for producing results
   *  from joining and grouping samples.
   */

  object SFOut {
  
    // For use with joins

    val LEFT = (s: Sample, t: Sample) => s

    val RIGHT = (s: Sample, t: Sample) => t

    val OVERWRITE = overwritef()

    def overwritef(f: String => String = { "r." + _ } ) =
      (s: Sample, t: Sample) => s ++ t.renameMeta(f).meta

  }  // End object SFOut
      


  object implicits
  {
    /** Functions to facilitate more readable syntax when
     *  using the above to express queries.
     */

    implicit def metaS[A](k: String): Sample => A = 
      (s: Sample) => s.getMeta[A](k)

    implicit def joinbyS(k: String): SAMPLEPRED = 
      (s: Sample, t: Sample) => s(k) == t(k)

    implicit  def aggrS[A](a: AGGR[Bed, A]) = (s: Sample) => a(s.track)
    // Compute an aggregate function on a sample's track


    // Tricks to endow Sample with attributes used by GMQL Samples

    implicit class SampleWithCell(s: Sample) {
      def cell: String = s.getStr("cell") 
    }

    implicit class SampleWithDB(s: Sample) {
      def db: String = s.getStr("db") 
    }

    implicit class SampleWithTF(s: Sample) {
      def tf: String = s.getStr("tf") 
    }


    /** Implicits copied from [[BEDFileOps]]
     */

    implicit def metaR[A](k: String) = (b: Bed) => b.getMeta[A](k)

    implicit class BedWithPeak(b: Bed) { 
      def peak: Int = b.getInt("peak") 
    }

    implicit class BedWithSval(b: Bed) { 
      def signalval: Double = b.getDbl("signalval")
    }

    implicit class BedWithPval(b: Bed) {
      def pval: Double = b.getDbl("pval")
    }

    implicit class BedWithQval(b: Bed) {
      def pval: Double = b.getDbl("Qval")
    }


    /** Implicits copied from [[DBFile]]
     */

   implicit def OSeqToCBI[B,K](ocoll: OSeq[B,K]): CBI[B]  = ocoll.cbi

    implicit def OSeqFromIT[B](it: IterableOnce[B]): OSeq[B,Unit] = 
      OSeq[B,Unit](it, (x:B)=>())(Ordering[Unit])

    implicit def OFileToCBI[B,K](ofile: OFile[B,K]): CBI[B] = ofile.cbi

  }  // End object implicits

} // SAMPLEFileOps




/** Examples ****************************************************
 *
  {{{


import scala.language.implicitConversions
import gmql.SAMPLEFileOps._
import gmql.SAMPLEFileOps.implicits._
import gmql.BEDFileOps._
import gmql.BEDFileOps.Genometric._
import gmql.BEDFileOps.BFAggr._
import gmql.BEDFileOps.BFCover._
import dbmodel.DBModel.{ Predicates, OpG }
import dbmodel.DBFile.OFile

//
// Import either SequentialGMQL or ParallelGMQL, but not both.
//
// Depending on which version of Scala you have, 
// some parallel operations may hang Scala, esp. the REPL.
// This is apparent due to Scala's default lazy initialization
// of static functions and objects.  This can usually be solved 
// using this Scala REPL option:
//
//       scala -Yrepl-class-based
//
// https://github.com/scala/scala-parallel-collections/issues/34


// import gmql.SAMPLEFileOps.SequentialGMQL._
import gmql.SAMPLEFileOps.ParallelGMQL._


dbmodel.DBFile.TMP       = "."    // Use current directory to store temp files
gmql.SAMPLEFileOps.ANS   = "."    // Use current directory to store results.
gmql.BEDFileOps.STORE    = true   // If true, always serialize BED files.
gmql.SAMPLEFileOps.STORE = true   // If true, always serialize Sample files.



// Define printing functions to display Samples and BED files.

implicit class OFilePrinter[B,K](ofile: OFile[B,K]) {
  def print(n: Int) = ofile.cbi.done {
    // Use .done to ensure file is autoclosed at end.
    _.take(n).foreach(s => { println(s"### $s ###\n") })
  }
}

implicit class SampleFilePrinter(samples: SAMPLEFILE) {
  def print(n: Int) = OFilePrinter(samples).print(n)
  def print(n:Int, m:Int) = samples.cbi.done {
    _.take(n).foreach { s => println(s"## $s ##\n"); s.bedFile.print(m) }
  }
}



// Read an ENCODE Sample file

val ctcf = {
  val path     = "test/hepg2_np_ctcf/files"
  val filename = "test/ctcf-list.txt"
  SampleFile.gmqlEncodeSampleFile(path, filename)
}


val acopy = ctcf.deepSaveAs("wlstest") 
  // Make a totally fresh copy which
  // is protected by default.


acopy.protection(false).close()
  // Unprotect and destroy it


val bcopy = ctcf.serialized
  // Make a totally fresh copy which
  // is unprotected by default.


bcopy.close()
  // Destroy it.


ctcf.print(2,3)
  // print 3 lines of ctcf(0) & ctcf(1).
                                       



// Select, object-oriented version

{ val qry1 = ctcf.selectS { _.sid startsWith "1375" }

  qry1.done { cbi => print("*** sid of the first sample is "); 
                   println(cbi.head.sid) }

    // .done(f)         --- apply f to the query result, 
    //                      then delete the query result.
    // .serialized      --- put the query result as a tmp file on disk.
    // .deepSaveAs(f,d) --- save the query result as file f in folder d. 
}


{ val qry2 = ctcf.selectS( _.sid startsWith "1375" )

  val saved = qry2.serialized
    // Result is saved to a temp file.

  saved.print(1)
    // Display the first entry in the saved results.

  saved.close()
    // Temp files are unprotected. So, .close() deletes it for good.
}




// Select, non-OO version

{ val qry3 = SFOps.selectS { _.sid startsWith "1375" } (ctcf)
  qry3.print(1) 
  qry3.close()
}




// Project, OO version

{ // "Chain" together projectS and selectS. Notice that there is
  // no .done/.materialized/.deepSaveAs between the projectS and
  // selectS subqueries. This means no intermediate files are
  // generated---the result of projectS is directly streamed to
  // selectS.
 
  val qry4 = ctcf.projectS("sid" -> (_.sid), "len" -> (_.bedFile.length))
                 .selectS { _.getInt("len") > 50000 }
  qry4.print(3)
  qry4.close()
}



{ // This example demos a nice trick. An implicit class is used to
  // endow Sample with a "len" attribute. This way, you can say
  // s.len instead of s.getMeta[Int]("len") or s.getInt("len").

  implicit class SampleWithLen(s: SAMPLE) { def len: Int = s.getInt("len") }

  val qry5 = ctcf.projectS( "sid" -> (_.sid), "len" -> (_.bedFile.length))
                 .selectS( _.len > 50000)
  qry5.print(3)
  qry5.close()
}



// Project, non-OO version

{ val qry6 = SFOps.projectS("sid" -> "sid", "len" -> (_.bedFile.length))(ctcf)
  val qry7 = SFOps.selectS { _.getInt("len") > 50000 } (qry6)
  qry7.print(3) 
  qry7.close()
}



{ // Subqueries can be composed into a big query.

  val sel  = SFOps.selectS { _.getInt("len") > 50000 }  
  val prj  = SFOps.projectS("sid" -> "sid", "len" -> (_.bedFile.length)) 
  val qry8 = (sel compose prj)(ctcf)
  qry8.print(3) 
  qry8.close()
}



// Combine sample selection and BED file selection, OO version.

{ val qry9 = ctcf.selectS  { _.sid startsWith "1375" }
                 .onRegion { _.selectR { _.chrom == "chr22" } }

  val saved = qry9.deepSaveAs("qry9")
    // Save the result, so that we can query it many times.

  saved.print(1, 3)
  saved.protection(false).close()
}




// Combine sample selection and BED file 
// selection and projection, OO version.

{ val qry10 = ctcf.selectS  { _.sid startsWith "1375" }
                  .onRegion { _.selectR { _.chrom == "chr22" } }
                  .onRegion { _.projectR(
                                  "signalval"  -> (_.signalval),
                                  "regionsize" -> { b => b.end - b.start }) }
  
  qry10.done { _.foreach { s => println(s.sid, s.bedFile.length) } }
}


// Same as qry10, but the region selection and projection are combined.

{ val qry10a = ctcf.selectS  { _.sid startsWith "1375" }
                   .onRegion { _.selectR { _.chrom == "chr22" } 
                                .projectR(
                                  "signalval"  -> (_.signalval),
                                  "regionsize" -> { b => b.end - b.start }) }
  
  qry10a.done { _.foreach { s => println(s.sid, s.bedFile.length) } }
}


// Same as qry10, but non-OO version.

{ val qry11 = (SFOps.selectS { _.sid startsWith "1375" } 
       andThen SFOps.onRegion { BFOps.selectR { _.chrom == "chr22" } }
       andThen SFOps.onRegion { 
                  BFOps.projectR("signalval" -> (_.signalval),
                                 "regionsize" -> { b => b.end - b.start })}
       ) (ctcf)
  
  qry11.done { _.foreach { s => println(s.sid, s.bedFile.length) } }
}




// Extend, OO version


{ val qry12 = ctcf.selectS { _.sid startsWith "1375" }
                  .onRegion { _.selectR { _.chrom == "chr22" } }
                  .onRegion { _.extendR (
                                 "regionsize" -> { b => b.end - b.start }) }
  
  qry12.done { _.foreach { s => println(s.sid, s.bedFile.length) } }
}


{ val qry13 = ctcf
              .selectS { _.sid startsWith "1375" }
              .onRegion { _.selectR { _.chrom == "chr22" } }
              .onRegion { _.extendR("regionSz" -> { b => b.end - b.start }) }
              .serialized
              .extendS("len" -> { _.bedFile.length } )
  
  // Note that the "serialized" in the middle of qry13 above is crucial.
  // This is because the line following it "extendS(...)" consumes all
  // entries in the associated BED file. Serializing before executing
  // "extendS(...)" makes the BED file persistent, so that it is
  // available for subsequent use.  Otherwise, the "qry13.done ..."
  // below will say the s.bedFile.length is 0, as the bedFile was
  // already fully consumed when determining its length in qry13 above.

  qry13.done { _.foreach { s => { println(s.sid, s.bedFile.length) } } }
}


{ // However, the temp files created by the .serialized in qry13 are not
  // cleaned up.  You can rewrite this slightly to clean up the temp files.
  // The "val qry13b = ... .use(qry13a)" records the fact that qry13a is
  // a resource for producing qry13b, and should be released automatically
  // when qry13b is closed/done. 

  val qry13a = ctcf
               .selectS { _.sid startsWith "1375" }
               .onRegion { _.selectR { _.chrom == "chr22" } }
               .onRegion { _.extendR("regionSz" -> { b => b.end - b.start }) }
               .serialized
  val qry13b = qry13a
               .extendS("len" -> { _.bedFile.length } )
               .use(qry13a)
  qry13b.done { _.foreach { s => { println(s.sid, s.bedFile.length) } } }

  // Actually, I bluff. Our engine automatically tracks the provenance of
  // a query result. When a query result is closed/deleted, temp files
  // in its chain of provenance get auto-closed/deleted as well.
}




// Extend, non-OO version

{ val qry14 = SFOps.extendS("len" -> { _.bedFile.length } )(ctcf)
  qry14.print(3) 
  qry14.close()
}




// Groupby, OO & non-OO version

{ val qry15 = ctcf.groupbyS("cell", "count" -> OpG.COUNT[SAMPLE])
  qry15.print(10) 
  qry15.close()
}



{ val qry16 = SFOps.groupbyS("cell", "count" -> OpG.COUNT[SAMPLE])(ctcf)
  qry16.print(10) 
  qry16.close()
}




// Difference, OO & non-OO version

{ // Remove chr1 from ctcf BED files

  val chr1  = ctcf.onRegion { _.selectR { _.chrom == "chr1" } } 
  val qry17 = ctcf differenceS chr1
  qry17.print(2, 3) 
  qry17.close()
}



{ val chr1  = SFOps.onRegion { _.selectR { _.chrom == "chr1" } } (ctcf) 
  val qry18 = SFOps.differenceS(exact = false)(ctcf, chr1)
  qry18.print(2, 3) 
  qry18.close()
}



// Map, OO and non-OO version


{ // The OO version is able to use default parameters.
  // So, it is very short.

  val qry19 = ctcf mapS ctcf

  qry19.done { _.foreach { s => print("sid: " + s.sid + ", len = "); 
                                println(s.bedFile.length) } }
}



{ // The same query as qry19, but defaults made explicit.

  val qry20 = ctcf.mapS(ctcf)(onRegion = BFOps.mapR(),
                              onSample = SFOut.LEFT)

  qry20.done { _.foreach { s => print("sid: " + s.sid + ", len = "); 
                                println(s.bedFile.length) } }
}



{ // The non-OO version does not support defaults.
  // So, all parameters must be provided.

  val qry21 = SFOps.mapS(onRegion = BFOps.mapR(),
                         onSample = SFOut.LEFT) (ctcf, ctcf)

  qry21.done { _.foreach { s => print("sid: " + s.sid + ", len = "); 
                                println(s.bedFile.length) } }
}



{ // A variety of aggregate functions can be used with mapR.
  // E.g., COUNT, SUM, AVERAGE, SMALLEST, and BIGGEST.

  val onRegion = BFOps.mapR("ave"  -> AVERAGE(_.signalval),
                            "peak" -> SMALLEST(_.peak))

  val qry22 = ctcf.mapS(ctcf)(onRegion = onRegion, onSample = SFOut.LEFT)
  qry22.print(2, 3)
  qry22.close()

}
  


// Cover and related queries.


{ // BFOps.{ coverR, flatR, summitR, histoR, complementR } can be
  // used as the onRegion parameter value for coverS(...). In fact,
  // any function of type Seq[BEDFILE]=>BEDFILE can be used.
  //
  // The BFOps are themselves parameterized by BETWEEN(m,n), ATMOST(n),
  // ATLEAST(n), EXACTLY(n), and any function of type Int=>Bool.
  // They can also support any number of "name -> aggregate function"
  // parameters.
 
  val onRegion = BFOps.coverR(BETWEEN(2,3), "signal" -> BIGGEST("signalval"))
  val qry23    = ctcf.coverS(onRegion = onRegion, groupby = "cell")
  qry23.print(2, 3)
  qry23.close()
}


{ // The groupby parameter is optional.
  val qry24 = ctcf.coverS(BFOps.complementR)
  qry24.print(2, 3) 
  qry24.close()
}


{ // You can also groupby multiple metadata.
  val qry25 = ctcf.coverS(BFOps.summitR(ATLEAST(1)), "cell", "tf")
  qry25.print(2, 3)
  qry25.close()
}


{ // And here is a non-OO version.
  val qry26 = SFOps.coverS(BFOps.histoR(ATLEAST(1))) (ctcf)
  qry26.print(2, 3)
  qry26.close()
}


// Join


{ // BFOps.joinR(ANTIMONOPRED, GENERALPRED*)(OUTPUT, ORDER) is the join
  // operator on two BED files.  
  //
  // ANTIMONOPRED is any antimonotonic functions (Locus,Locus) => Bool. 
  // Examples are Genometric.{ OVERLAP(n), OVERLAPSTART, NEAR(n), DL(n),
  // DLEQ(n), EQ, LT, LTEQ, SZPERCENT(n), BEFORE, STARTBEFORE }
  // Multiple predicates can be conjunctively combined as
  // Predicates.requires(OVERLAP(n), STARTBEFORE).
  //
  // GENERALPRED is any general predicate (Bed,Bed)=> Bool. These include
  // Genometric.{ STARTAFTER, ENDBEFORE, ENDAFTER, OVERLAPEND, FAR(n),
  // INSIDE, OUTSIDE, ENCLOSE, TOUCH, GT, GTEQ, DG, DGEQ }. These can also
  // be conjunctively combined, including combining with ANTIMONOPRED;
  // e.g. Predicates.requires(STARTAFTER, NEAR(1000)).
  //
  // OUTPUT specifies how to combined the metadata of two matched BED 
  // entries. Valid options are BFOut.{ LEFT, RIGHT, INT, CAT, BOTH }.
  // However, any function (Bed,Bed)=>Option(Bed) can be used.
  //
  // ORDER specifies how to sort/order the output BED file. Valid options
  // are BFOrder.{ NONE, DISTINCT, SORT, SORTDISTINCT, PLUS, MINUS,
  // NULLSTRAND, HISTO(minmax), SUMMIT(minmax), COVER(minmax),
  // FLAT(minmax), COMPLEMENT }. However, any function having type
  // BEDITERATOR=>BEDITERATOR can also be used. 
  //
  // samples.joinS(otherSamples, joinby*)(onRegion, onSample) means the
  // following in SQL:
  //
  // SELECT Sample(onSample(s, t), onRegion(s.bedFile, t.bedFile))
  // FROM samples as s, otherSamples as t
  // WHERE joinby(s,t)
  //
  // onRegion is joinR(...), the join of s and t's BED files. See above.
  //
  // onSample is a function for combining s and t's metadata. Valid options
  // are SFOut.{ OVERWRITE, LEFT, RIGHT }. But any function having type
  // (Sample,Sample)=>Sample can be used.

  val qry27 = ctcf.joinS(ctcf)(
                onRegion = BFOps.joinR(OVERLAP(1))(BFOut.BOTH),
                onSample = SFOut.OVERWRITE)

  qry27.print(2,3)
  qry27.length
  qry27(3).bedFile.length
  qry27.close()
}


{ // Non-OO version of qry27

  val qry28 = SFOps.joinS(
                onRegion = BFOps.joinR(OVERLAP(1))(BFOut.BOTH),
                onSample = SFOut.OVERWRITE) (ctcf, ctcf)

  qry28.print(2,3)
  qry28.length
  qry28(3).bedFile.length
  qry28.close()
}


  }}}
 *
 */



