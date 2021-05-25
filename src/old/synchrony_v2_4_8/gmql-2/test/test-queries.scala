
// Test script for Synchrony GMQL
//
// Wong Limsoon
// 18/10/2020
//


/** Set up the execution environment for Synchrony GMQL.
 *  There are three environments:
 *  Default            - Query results are produced in a stream-like
 *                       iterator-like manner. I.e., query results
 *                       are accessed like an iterator: produced
 *                       an item at a time, on-demand, and read-once-only.
 *  AlwaysSortIfNeeded - implicitly sorts query results.
 *  AlwaysMaterialize  - implicitly materializes query results.
 *
 * Please import exactly once of these environment. I prefer the 
 * default environment since it gives me full control. However,
 * AlwaysMaterialize is recommendeded for a less-experienced user.
 */

// import synchrony.gmql.SampleFileOps._
// import synchrony.gmql.SampleFileOpsImplicit.AlwaysSortIfNeeded._

import synchrony.gmql.SampleFileOpsImplicit.AlwaysMaterialize._


/** This is a package for doing timing studies. It mainly
 *  provides a function,
 *    def nRuns(n: Int, t: Timings = new Timings)(samples: => SampleFile)
 *  which runs the query (samples) n times, and accumulates the 
 *  run timings into the Timings structure t. Later, you can get
 *  the timing result by accessing t.stats.
 *
 */

// import synchrony.gmql.SampleFileOpsTimings._


/** These are packages for functions on BED-formatted files.
 *  You need them to write queries on samples.
 */

import synchrony.genomeannot.BedFileOps._
import synchrony.genomeannot.BedFileOps.BFOps._
import synchrony.genomeannot.GenomeAnnot.GenomeLocus._

/** OpG is a package that provides commonly used aggregate functions.
 *  We just import a few of these as examples.
 */

import OpG.{ count, average, smallest, stats }


/** Parser for importing samples of ENCODE Narrow Peak files
 *  prepared in GMQL sample file format, into our self-describing
 *  sample file format.
 *
 *  Example: {{{
      val db = importEncodeNPSampleFile(dbpath)(dblist)("db-xyz")
 *  }}}
 *
 *  Note that the above should be done only once. It creates "db-xyz"
 *  on disk. After that, "db-xyz" can be used in future programs via:
 *     val db = onDiskSampleFile("db-xyz") 
 *  I.e., dont need to import again.
 */

import synchrony.gmql.EncodeNP._



/** Some functions for showing contents of samples.
 */

def showSamples(samples: SampleFile) =
  for(s <- samples) { 
    println(s)
    println("**")
  }


def showTotalSizeOnDisk(samples: SampleFile) = samples.totalsizeOnDisk


def showTrack(sample:Sample) = for(e <- sample.track) println(e) 


def showSize(sample:Sample) = {
  val size = sample.bedFile.filesize
  val count = sample.track.length
  println(s"track size = ${size}, track count = ${count}")
}


def showTiming[A](codes: => A): A = {
  val t0 = System.nanoTime;
  val res = codes
  val dt = (System.nanoTime - t0) / 1e9d;
  println(s"Time take: ${dt}")
  res
}


/** Here is how you import a sample file from GMQL.
 *  In the example below,
 *  ctcfPath is a directory of samples.
 *  ctcfList is a list of samples that you want to import.
 *
 *  Note that a "val" is used. This ensures the samples
 *  are imported only once. If a "def" was used, the samples
 *  would be imported fresh every time ctcfdb is used.
 */

val ctcfdb = {
  val dir = "../../synchrony-1/test/test-massive/"
  val ctcfPath = dir + "cistrome_hepg2_narrowpeak_ctcf/files"
  val ctcfList = dir + "ctcf-list.txt"
  importEncodeNPSampleFile(ctcfPath)(ctcfList)("ctcfdb")
}


// The next three lines, depends on whether you are using
// the Default or the AlwaysMaterialize environment.
 
ctcfdb(0) // In both environments, this gives the 1st item of the result.

ctcfdb(0) // In Default environment, this gives the 2nd item of the result;
          // recall, the default environment is stream/iterator-like.
          // In AlwaysMaterialize, this gives the 1st item of the result.

ctcfdb(3) // In the Default enviroment, if we didnt access ctcfdb(0) twice
          // earlier on, this would give the 4th item of the result. Since
          // we accessed ctcfdb(0) twice, this now gives the 5th item.
          // In AlwaysMaterialize, this gives the 4th item.


// This query merges the BED file of ctcfdb(2) and ctcfdb(3), 
// and then replaces the BED file of ctcfdb(0) with this.

val mm = ctcfdb(0).bedFileUpdated {
  ctcfdb(2).bedFile mergedWith ctcfdb(3).bedFile
}

ctcfdb(2).track.length
ctcfdb(3).track.length
mm.track.length   // the first two numbers should sum to the third number.



/** Before example queries are given, let's first review the 
 *  structure of samples and their tracks (genomic regions.) 
 *  Conceptually, a GMQL sample file can be thought of as a
 *  simple 1-level nested relation, 
 *
 *     EFile[Sample(l1, .., 
 *                  ln, 
 *                  track: EFile[Bed(chrom, 
 *                                   chromStart,
 *                                   chromEnd,
 *                                   strand,
 *                                   name
 *                                   score,
 *                                   r1, ..., rm))])]
 *  where l1, .., ln are metadata attributes of a sample;
 *        r1, .., rm are metadata attributes of a genomic region (
 *                       a row in a BED file.
 *
 *  EFile can be thought of as a "file-based vector",
 *  which transparently materializes the parts of the
 *  vector of samples and BED rows into memory when
 *  they are needed in a computation.
 *
 *  The main query operations provided by GMQL are mirrored
 *  after the relational algebra (SELECT, PROJECT, JOIN,
 *  GROUPBY) on samples (ignoring the track attribute; i.e.
 *  the BED file); aggregate functions on the track attribute;
 *  and if we regard the track attribute/BED file as a 
 *  relational table, GMQL provides also relational algebra
 *  operations (SELECT, PROJECT, JOIN, GROUPBY) on the 
 *  track/BED file, as well as operations with specialized
 *  genomic meaning (MAP, DIFFERENCE, and COVER) on the
 *  track/BED file. 
 */


/** SELECT query on regions (i.e. BED file rows), selectR ...
 *
 *  us.selectR(p) is roughly the following ,pseudo SQL query
 *  on us:BedFile,
 *
 *      SELECT u.* from us u WHERE p(u)
 *
 *  db.onRegion(f) applies the function f:BedFile=>BedFile
 *  to every sample in db:SampleFile. 
 */

val q2 = ctcfdb.onRegion( _.selectR( _.chrom == "chr3"))

showSamples(q2)
showTrack(q2(1))  // Display the track of sample #1 in q2.
for(s <- q2) println(s.track.length)
q2(0).bedFile(8)


/** SELECT query on sample, selectS ... 
 *
 *  db.selectS(f) is roughly the following pseudo SQL query
 *  on db:SampleFile,
 *
 *  SELECT s.* FROM db s WHERE f(s)
 *
 *  That is, it selects samples in db satisfying the predicate
 *  f:Sample=>Boolean.
 */

// Both q1a and q1b do the samething. q1b is less verbous.

val q1a = ctcfdb.selectS(onSample = _.track.length > 50000)

val q1b = ctcfdb.selectS(_.track.length > 50000)

for(s <- ctcfdb) println(s.track.length)
for(s <- q1a) println(s.track.length)
for(s <- q1b) println(s.track.length)




/** EXTEND query on samples, extendS ...
 *
 *  db.extendS(l1 -> f1, .., ln -> fn) is this pseudo SQL query 
 *  on db:SampleFile,
 *
 *  SELECT s.*, l1 as f1(s), .., ln as fn(s) FROM db s
 *
 *  The functions f[A]:Sample=>A are arbitrary functions on samples.
 *  The l are strings to be used as new metadata names.
 */

// q3a adds metadata "bed-count" (defined as size of a sample's
// bed file) and "my-sid" (defined as a sample's sid) to each 
// sample.

val q3a = ctcfdb.extendS( "bed-count" -> count[Bed], "myid" -> "sid")

q3a(0)



/** PROJECT query on samples, projectS ...
 *
 *  db.projectS(l1 -> f1, .., ln -> fn) is this pseudo SQL query
 *  on db:SampleFile,
 *
 *  SELECT l1 as f1(s), .., ln as fn(s) FROM db s
 *
 *  The functions f[A]:Sample=>A are arbitrary functions on samples.
 *  The l are strings to be used as new metadata names.
 */

// q3b adds metadata "bed-count" (defined as size of a sample's
// bed file) to each sample, and removes all other metadata
// of the sample.

val q3b = ctcfdb.projectS( "bed-count" -> count[Bed])

showSamples(q3b)
for(s <- q3b) println(s[Double]("bed-count"))


// q4 adds metadata "bed-count" (defined as size of a sample's
// bed file) to each sample, and metadata "sid" (defined as the
// sample's sid), and removes all other metadata of the sample.
//
// Note that extendS and projectS are similar. However, extendS
// leaves other metadata along, while projectS removes them.

val q4 =  ctcfdb.projectS( "bed-count" -> count[Bed], "sid" -> "sid")

showSamples(q4)
for(s <- q4) println(s[String]("sid"), s[Double]("bed-count"))


/** MAP query on samples and regions, mapS and mapR.
 *
 *  db1.mapS(db2)(f, g, joinby) is this pseudo SQL query 
 *  on db1:SampleFile and db2:SampleFile,
 *
 *  SELECT g(s, t).bedFileUpdated(f(s.BedFile, t.BedFile))
 *  FROM db1 s, db2 t
 *  WHERE joinby(s,t)
 *
 *  g: (Sample, Sample) => Sample,
 *  f: (BedFile, BedFile) => BedFile,
 *  joinby: (Sample, Sample) => Boolean
 *
 *  can be any functions of the right type. In particular,
 *  f:(BedFile,BedFile)=>BedFile can be a mapR query on
 *  BED files.
 *
 *  mapR(k1 -> a1, .., kn -> an)(ctr)(us, vs) is roughly
 *  this pseudo SQL query on us:BedFile and vs:BedFile,
 *
 *  SELECT u.*, ctr as COUNT, k1 as A1, .., kn as An
 *  FROM us u, vs v
 *  WHERE u overlaps v 
 *  GROUPBY locus of u
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group,
 *       COUNT is size of the group
 *
 *  ctr, k1, ..,kn are Strings to be used as names of new metadata;
 *  ctr is implicit and can be omitted.
 *  a:Aggr[Bed,Double] are aggregate functions on BedFile;
 *  common aggregate functions can be imported from the OpG package.
 */

val mapres2 = ctcfdb.mapS(ctcfdb)(
  onRegion =  mapR("avg_score" -> average(_.score), 
                   "stats" -> stats(_.score))
)


mapres2(0)
mapres2(0).bedFile(8)


val mapres3 = ctcfdb.mapS(ctcfdb)(
  onRegion =  mapR("avg_score" -> average(_.score))
)

mapres3(0)
mapres3(0).bedFile(8)


val mapres = ctcfdb.mapS(ctcfdb)(
  onRegion = mapR("avg_score" -> average(_.score)),
  joinby = (u, v) => u("sid") == v("sid")
)


showSamples(mapres)
showTrack(mapres(0))
showSize(mapres(0))
mapres(0)
mapres(0).bedFile(9)


/** GROUPBY query on samples, groupbyS ...
 *
 *  db.groupbyS(g, l1 -> a1, .., ln -> an) is roughly this pseudo
 *  SQL query on db:SampleFile,
 *
 *  SELECT s.*, group as s.g, l1 as A1, .., ln as An
 *  FROM db s
 *  GROUPBY s.g
 *  WITH A1 = results of aggregate function a1 on a group, ..,
 *       An = results of aggregate function an on a group
 *
 *  g:String is a sample attribute to be used for grouping samples in db.
 *  l:String are names of new metadata (the aggregate function results.)
 *  a:Aggr[Sample,Double] are aggregate functions on samples.
 */

val grpbysid = mapres3.groupbyS(grp  = "sid", 
                                aggr = "count" -> count[Sample])

// val grpbysid = mapres3.groupbyS("sid", "count" -> count[Sample])
// is a less verbous version of the same query.

showSamples(grpbysid)
showTrack(grpbysid(1))
grpbysid(0)
grpbysid(0).bedFile(6)



/** GROUPBY query on Bed files, partitionbyR ...
 *
 *  partitionbyR(g, k1 -> a1, .., kn -> an)(us) is roughly this
 *  pseudo SQL query on us:BedFile,
 *
 *  SELECT u.chrom, u.chromStart, u.chromEnd, u.g, k1 as A1, .., kn  as An
 *  FROM us u
 *  GROUPBY u.chrom u.chromStart, u.chromEnd, u.g
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group.
 *
 *  g:String is an optional Bed attribute to be used, 
 *  in addition to loci, for grouping entries in Bed file.
 *  k:String are names of new metadate (the aggregate function results.)
 *  a:AGGR[Bed,Double] are aggregate functions to be applied on a group.
 */

val grpbylocus = mapres3.onRegion(
  partitionbyR(
    aggr = "min-score" -> smallest(_.score),
           "reg-count" -> count)
)


showSamples(grpbylocus)
showSize(grpbylocus.samples(1))
grpbylocus(1).bedFile(5)  // Display region #5 on sample #1's track


{ // test in grpbylocus whether sample #1's track is sorted.
  import synchrony.programming.Sri.withSlidingN
  val leq = Bed.ordering.lteq _
  grpbylocus(1).bedFile flatAggregateBy { withSlidingN(1) {
    OpG.forall { case (x, y) => leq(x(0), y) } } }
}
 


/** DIFFERENCE of samples' tracks/regions, differenceS and differenceR ...
 *
 *  db1.differenceS(db2)(joinby, exact) is roughly this pseudo
 *  SQL query on db1:SampleFile and db2:SampleFile,
 *
 *  SELECT s.*, 
 *         differenceR(exact)(s.BedFile,
 *                            SELECT t.BedFile FROM db2 t WHERE joinby(s, t))
 *  FROM db1 s
 *
 *  differenceR(exact)(us, d1, .., dn) is this pseudo SQL query on
 *  us:BedFile, d1:BedFile, ...dn:BedFile,
 *
 *  SELECT u.*
 *  FROM us u
 *  WHERE exact && locus of u is not in any of d1, .., dn
 *     OR (!exact) && locus of u does not overlap any region in d1, .., dn.
 */

val diffdb = {
  val refdb = SampleFile.inMemorySampleFile {
     ctcfdb.eiterator.take(1).toVector
  } // refdb is sample #1 in ctcfdb.

  val extdb = SampleFile.inMemorySampleFile {
     ctcfdb.eiterator.drop(1).toVector
  } // extdb is the rest of ctcfdb.

  refdb.differenceS(extdb)(exact = false)
}


showSamples(diffdb)
showTrack(diffdb(0))
showSize(ctcfdb(0))
showSize(diffdb(0))
diffdb(0)
diffdb(0).bedFile(9)


/** JOIN query on samples and regions, joinS and joinR ...
 *
 *  db1.joinS(db2)(f, g, joinby) is roughly this pseudo SQL query
 *  on db1:SampleFile and db2:SampleFile,
 *
 *  SELECT g(s, t).bedFileUpdated(f(s.BedFile, t.BedFile))
 *  FROM db1 s, db2 t
 *  WHERE joinby(s,t)
 *
 *  g: (Sample, Sample) => Sample,
 *  f: (BedFile, BedFile) => BedFile,
 *  joinby: (Sample, Sample) => Booleabn
 *
 *  can be any functions of the right type. In particular,
 *  f:(BedFile,BedFile)=>BedFile can be the joinR function
 *  for a join query on BED files.
 *
 *  joinR(ycs1, .., ncs1, ..)(output, screen, ordering)(us, vs)
 *  is this pseudo SQL query on us:BedFile and vs:BedFile,
 *
 *  SELECT output(u, v)
 *  FROM us u, vs v
 *  WHERE ycs1(v, u) && ycs2(v, u) && ..
 *        ncs1(v, u) && ncs2(v, u) && ..
 *        screen(u, v)        
 *  ORDERBY ordering
 *
 *  output: (Bed, Bed) => Option[Bed],
 *  screen: (Bed, Bed) => Boolean,
 *  ordering: Iterator[Bed] => BedFile
 *  ycs, ncs: LocusPred
 *
 *  can be any functions of the right type. In particular,
 *  ycs - convex genometric predicates (e.g. DLE(k), DL(k), Overlap(k));
 *  ncs - non-convex genometric predicates (e.g. DGE(k), DG(k));
 *  output - BFOut.{ both, left, right, cat, intersect }
 *  ordering - BFOrder.{ none, distinct, sort, sortDistinct, 
 *                       plus (for keeping only non -ve strand),
 *                       minus (for keeping on non +ve strand),
 *                       flat (for merging overlapping regions),
 *                       complement (for producing gaps in the regions),
 *                       summit (for keeping "summit" regions based on
 *                               the "accIndex" metadata of regions) };
 */

val joindb = ctcfdb.joinS(ctcfdb)(
  onRegion = joinR(geno = DGE(5000), DLE(100000))
)

joindb.length
showSamples(joindb)
showSize(joindb(0))
joindb(1)
joindb(1).bedFile(8)



/** For convenience, certain implicit conversion functions
 *  are provided in the environment. Here are pairs of
 *  queries with and without implicit conversions...
 */

// Explicit:
val projres = ctcfdb.projectS(
  onSample = "sid" -> metaS[String]("sid"),
             "bed-count" -> aggrS(count)
)

// Relying on implicit conversion:
val projres2 = ctcfdb.projectS(
  onSample = "sid" -> "sid", 
             "bed-count" -> count[Bed]
)


{ // let's check whether projres and projres2 have the same values.
  val pr1 = projres.toVector
  val pr2 = projres2.toVector
  for (i <- 0 to pr1.length - 1;
       m = pr1(i).meta == pr2(i).meta;
       t = pr1(i).bedFile hasSameValueAs pr2(i).bedFile
  ) yield (i, m, t)
}


