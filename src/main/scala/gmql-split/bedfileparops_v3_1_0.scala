
package synchrony.pargmql


/** Synchrony GMQL
 *
 *  This package provides operations on tracks (genomic regions).
 *  The provided operations are based on those of GMQL, but I
 *  extracted here only those parts pertaining to tracks, discarding
 *  those parts pertaining to samples. GMQL mixes these two parts
 *  in a non-orthogonal way, which makes it less convenient to be
 *  used as a programming library on tracks (without the baggage
 *  on samples.)
 *
 *  Conceptually, a track can be regarded as a relational table,
 *    EFile[Bed(chrom,
 *              chromStart,
 *              chromEnd, 
 *              strand, 
 *              name, 
 *              score, 
 *              r1, .., rn)]
 *  where r1, .., rn are the metadata attributes on a region. 
 *  The EFile can be thought of as a "file-based vector" which
 *  transparently materializes the parts of the vector of samples
 *  and rows in the underlying BED file into memory when they
 *  are needed in a computation.
 *
 *  The operations on tracks/BED files provided by GMQL are 
 *  essentially relational algebra operations (SELECT, PROJECT,
 *  JOIN, GROUPBY) on EFile[Bed], as well as operations with
 *  specialized genomic meaning (MAP, DIFFERENCE, and COVER)
 *  on the track/BED file. 
 *
 *  MAP is actually equivalent to a JOIN+GROUPBY+aggregation
 *  function query: it compares two tracks (call these the
 *  landmark and experiment tracks), grouping the regions/BED
 *  entries in the experiment track to regions/BED entries that
 *  their loci overlap with in the landmark track,  applying
 *  some aggregate functions on each resulting group, and 
 *  finally producing for each region in the landmark track,
 *  the corresponding aggregate function results as the output.
 *
 *  DIFFERENCE is similar to MAP, but the landmark track is
 *  compared to multiple experiment tracks. The output is
 *  those regions in the landmark track that t overlap with
 *  no region in all the experiment tracks.
 *
 *  The JOIN on (two) tracks supports the so-called genometric
 *  predicates, which are essentially "greater/less than" 
 *  predicates on the distance between two regions in the 
 *  two tracks. It also supports a special genometric predicate,
 *  MD(k), which tests whether a one region/BED entry in one
 *  track is a k-nearest neighbour of another region/BED entry
 *  in another track in terms of genomic distance.
 *
 *  COVER is more complicated, and there is no obvious efficient
 *  relational query equivalent. In its simplest form COVER(n,m),
 *  applied to a set of tracks, outputs (sub)regions that 
 *  overlap with at least n and atmost m of the tracks.
 *
 *  Here we emulate (using Synchrony iterators) all operations 
 *  in GMQL, except the MD(k) predicate. Actually,
 *  for any specific GMQL querying involving MD(k), it is also
 *  expressible in the Synchrony iterator framework in a way
 *  that achieves computational efficiency. However, including 
 *  it makes the emulation codes not so nice looking; so I omit it.
 *
 *  The package implements the GMQL operations on tracks/BED files.
 *  The GMQL operations at the level of samples are provided in
 *  another package, SampleFileOps.  This facilitates
 *  the BedFileOps to be used separately (i.e. BED files), 
 *  without bundling together with samples (BED files can be
 *  used in other contexts.)
 *
 * Wong Limsoon
 * 17 April 2021
 */


object BedFileParOps {

  import synchrony.genomeannot.{ GenomeAnnot, BedWrapper, SplitFiles }
  import synchrony.genomeannot.BedFileOps
  import synchrony.iterators.{ SyncCollections, AggrCollections }
  import synchrony.iterators.FileCollections.EFile
  import synchrony.programming.Sri
  import synchrony.programming.Sri._

  val BOps = BedFileOps.BFOps

  val Locus = GenomeAnnot.GenomeLocus
  val Bed = BedWrapper.SimpleBedEntry
  val BedFile = BedWrapper.BedFile
  val SBedFile = SplitFiles.SBedFile
  val OpG = AggrCollections.OpG
  val EIterator = SyncCollections.EIterator
  
  type Bed = BedWrapper.SimpleBedEntry
  type BedFile = BedWrapper.BedFile
  type BedEIterator = BedWrapper.BedEIterator
  type SBedFile = SplitFiles.SBedFile
  type LocusLike = GenomeAnnot.LocusLike
  type LocusPred = Locus.LocusPred
  type EIterator[A] = SyncCollections.EIterator[A]

  // Type of aggregate function C => F
  type Aggr[C, F] = Sri[C, F] 



  implicit class BFOps(us: BedFile) {

    def selectR(pred: Bed => Boolean): BedFile = 
      BFOps.selectR(pred)(us)

    def selectR(bpred: Bed => Boolean, cpred: String => Boolean): BedFile =
      BFOps.selectR(bpred, cpred)(us)

    def projectR(proj: (String, Bed => Any)*): BedFile = 
      BFOps.projectR(proj: _*)(us)

    def extendR(proj: (String, Bed => Any)*): BedFile = 
      BFOps.extendR(proj: _*)(us)

    def partitionbyR(aggr: (String, Aggr[Bed, Any])*): BedFile = 
      BFOps.partitionbyR(aggr: _*)(us)

    def partitionbyR(grp: String, aggr: (String, Aggr[Bed, Any])*): BedFile =
      BFOps.partitionbyR(grp, aggr: _*)(us)

    def mapR
      (that: BedFile)
      (aggr: (String, Aggr[Bed, Any])*)
      (implicit ctr: String = "count")
    : BedFile = BFOps.mapR(aggr: _*)(ctr)(us, that)

    def differenceR(diffs: BedFile*)(implicit exact: Boolean = false): BedFile =
      BFOps.differenceR(exact)(us, diffs)

    def joinR
      (that: BedFile)
      (geno: LocusPred*)
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = BFOps.limit)
    : BedFile = BFOps.joinR(geno: _*)(output, screen, ordering, limit)(us, that)

    def iNearestR
      (that: BedFile)
      (geno: LocusPred*)
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = BFOps.limit)
    : BedFile = { 
      BFOps.iNearestR(geno: _*)(nearest)(
        output, screen, ordering, limit)(us, that)
    }

    def oNearestR
      (that: BedFile)
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (geno: LocusPred*)
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = BFOps.limit)
    : BedFile = { 
      BFOps.oNearestR(nearest)(geno: _*)(
        output, screen, ordering, limit)(us, that)
    }


    def coverR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : BedFile = BFOps.coverR(minmax, aggr: _*)(stranded)(Seq(us))

    def summitR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : BedFile = BFOps.summitR(minmax, aggr: _*)(stranded)(Seq(us))

    def histoR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : BedFile = BFOps.histoR(minmax, aggr: _*)(stranded)(Seq(us))

    def flatR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : BedFile = BFOps.flatR(minmax, aggr: _*)(stranded)(Seq(us))

    def complementR(implicit stranded: Boolean = true)
    : BedFile = BFOps.complementR(stranded)(Seq(us))

  } // End class BFOps




  object BFOps {


/** selectR(f)(us) 
 *
 *  SELECT u.* FROM us u WHERE f(u) 
 */

    def selectR(pred: Bed => Boolean) = 
      SBedFile.lift(BOps.selectR(pred))


    def selectR(bpred: Bed => Boolean, cpred: String => Boolean) =
      SBedFile.condlift(BOps.selectR(bpred), cpred)



/** projectR(k1 -> f1, .., kn -> fn)(us) 
 *
 *  SELECT k1 as f1(u), .., kn as fn(u) FROM us u
 */

    def projectR(proj: (String, Bed => Any)*) =
      SBedFile.lift(BOps.projectR(proj: _*))


/** extendR(k1 -> f1, .., kn -> fn)(us)
 *
 *  SELECT u.*, k1 as f1(u), .., kn as fn(u) FROM us u
 */

    def extendR(proj: (String, Bed => Any)*) =
      SBedFile.lift(BOps.extendR(proj: _*))


/** partitionbyR(k1 -> a1, .., kn -> an)(us)
 *
 *  SELECT u.chrom, u.chromStart, u.chromEnd,
 *         k1 as A1, .., kn  as An
 *  FROM us u
 *  GROUPBY u.chrom u.chromStart, u.chromEnd
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group.
 */

    def partitionbyR(aggr: (String, Aggr[Bed, Any])*) =
      SBedFile.lift(BOps.partitionbyR(aggr: _*))


/** partitionbyR(g, k1 -> a1, .., kn -> an)(us)
 *
 *  SELECT u.chrom, u.chromStart, u.chromEnd, u.g, k1 as A1, .., kn  as An
 *  FROM us u
 *  GROUPBY u.chrom u.chromStart, u.chromEnd, u.g
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group.
 */

    def partitionbyR(grp: String, aggr: (String, Aggr[Bed, Any])*) =
      SBedFile.lift(BOps.partitionbyR(grp, aggr: _*))


/** mapR(k1 -> a1, .., kn -> an)(ctr)(us, vs)
 *
 *  SELECT u.*, ctr as COUNT, k1 as A1, .., kn as An
 *  FROM us u, vs v
 *  WHERE u overlaps v 
 *  GROUPBY locus of u
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group,
 *       COUNT is size of the group
 *
 *  Notice that the "SQL" query above is a join of EFile[Bed] us
 *  and vs. These flat files typically has tens or hundreds of 
 *  thousand of entries. As the join condition, overlap, 
 *  is comparing two intervals, a naive implementation has
 *  quadratic complexity. Even with binning of regions into
 *  intervals, it is still quadratic.
 *
 *  Here, we implement it using Synchrony iterator, 
 *
 *        hs = et.syncedWith(r)(bf, cs).filter(_ sameStrand r);
 *              
 *  performs synchronized iteration on the us and vs (et in
 *  the codes below and line above) flat files directly. 
 *  The complexity becomes essentially linear.
 */ 

    def mapR(aggr: (String, Aggr[Bed, Any])*)(implicit ctr: String = "count") =
      SBedFile.binlift(BOps.mapR(aggr: _*)(ctr))



/** differenceR(exact)(us, d1, .., dn)
 *
 *  SELECT u.*
 *  FROM us u
 *  WHERE exact && locus of u is not in any of d1, .., dn
 *     OR (!exact) && locus of u does not overlap any region in d1, .., dn.
 *
 *  The "SQL" above actually is a multi-table join, as d1, .., dn
 *  are also BED files. For exact loci matching this can be
 *  quite fast since it is easy to build index on loci.
 *  When exact = false, inexact matching (i.e. overlap) is 
 *  required. Then a naive implementation has complexity
 *  n * |us x di|, i.e. quadratic.
 *
 *  Here we use Synchrony iterator, to implement it:
 *
 *        hs = merged.syncedWith(r)(bf, cs).filter(overlap(_, r));
 * 
 *  It synchronizes iterations on all these files at once.
 *  The complexity is linear, n * |di| + |us|.
 */

    def differenceR(implicit exact: Boolean = false) = 
      SBedFile.binmultilift(BOps.differenceR(exact))



/** limit is a default value that controls the maximum
 *  distance from whichb one genomic region can see another.
 */

    val limit = 100000


/** joinR(ycs1, .., ncs1, ..)(output, screen, ordering)(us, vs)
 *
 *  SELECT output(u, v)
 *  FROM us u, vs v
 *  WHERE ycs1(v, u) && ycs2(v, u) && ..
 *        ncs1(v, u) && ncs2(v, u) && ..
 *        screen(u, v)        
 *  ORDERBY ordering
 *
 *  As can be seen, this is naively also a join on tables us and vs.
 *  The genometric predicates ycs1.., ncs1.., are constraints on
 *  the loci of regions u in us and v in vs; in particular, these
 *  are non-exact matching. So, naive implementation is quadratic.
 *
 *  Again, we use Synchrony iterator to implement it:
 *
 *              h <- et.syncedWith(r)(bf, cs);
 *
 *  which performs synchronized iterations on us and vs (et in the
 *  line above). The complexity becomes essentially linear.
 */

    def joinR
      (geno: LocusPred*)
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = limit)
    : (BedFile, BedFile) => BedFile = {
 
      SBedFile.binlift(BOps.joinR(geno: _*)(output, screen, ordering, limit))

    }


    def iNearestR
      (geno: LocusPred*)
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = limit)
    : (BedFile, BedFile) => BedFile = {

      SBedFile.binlift(
        BOps.iNearestR(geno: _*)(nearest)(output, screen, ordering, limit)
      )
    }


    def oNearestR
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (geno: LocusPred*)
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = limit)
    : (BedFile, BedFile) => BedFile = {

      SBedFile.binlift(
        BOps.oNearestR(nearest)(geno: _*)(output, screen, ordering, limit)
      )
    }



/** coverR(minmax, l1 -> a1, .., lk -> ak)(v1, .., vn)
 *
 *  Returns all (sub)regions appear in m times in v1, .., vn
 *  where minmax(m) holds. 
 *
 *  This is implemented quite cleverly here. v1, ..., vn are first
 *  merged in linear time, assuming there are sorted on loci already.
 *  Then the merged list of regions are mapped to a duplicate-free
 *  version of itself using Synchrony iterator in linear time.
 *  With the map, it is easy to check if the start of a region
 *  is overlapped/covered by m other regions. If so, take intersection
 *  of these m other regions and output the intersection. If not,
 *  just skip the region. Also, apply aggregate functions a1, .., ak
 *  to regions contributing to the intersection.
 */
 
    def mapCoverR
      (coverOp: Iterator[Bed] => BedFile,
       aggr: (String, Aggr[Bed,Any])*)
      (implicit
         overlap: (Bed, Bed) => Boolean = BFCover.overlapStart,
         screen: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.coverGenMin,
         stranded: Boolean = true)
    : Seq[BedFile] => BedFile = { 

      SBedFile.multilift(
        BOps.mapCoverR(coverOp, aggr: _*)(overlap, screen, stranded)
      )
    }



    def coverR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : Seq[BedFile] => BedFile = { 
      // Requires all BedFiles are sorted on loci, 
      mapCoverR(BFOrder.cover(minmax), aggr: _*)(
        BFCover.overlapStart, 
        BFCover.coverGenMin,
        stranded
      )
    }


    def summitR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : Seq[BedFile] => BedFile = {
      // Requires all BedFiles are sorted on loci, 
      mapCoverR(BFOrder.summit(minmax), aggr: _*)(
        BFCover.overlapStart, 
        BFCover.coverGenMin,
        stranded
      )
    }


    def histoR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : Seq[BedFile] => BedFile = {
      // Requires all BedFiles are sorted on loci, 
      mapCoverR(BFOrder.histo(minmax), aggr: _*)(
        BFCover.overlapStart, 
        BFCover.coverGenMin,
        stranded
      )
    }


    def flatR
      (minmax: Int => Boolean, aggr: (String, Aggr[Bed,Any])*)
      (implicit stranded: Boolean = true)
    : Seq[BedFile] => BedFile = {
      // Requires all BedFiles are sorted on loci, 
      mapCoverR(BFOrder.flat(minmax), aggr: _*)(
        BFCover.overlapStart, 
        BFCover.coverGenMax, 
        stranded
      )
    }


    def complementR(implicit stranded: Boolean = true)
    : Seq[BedFile] => BedFile = {
      // Requires all BedFiles are sorted on loci, 
      mapCoverR(BFOrder.complement)(
        BFCover.overlapStart, 
        BFCover.coverGenMax,
        stranded
      )
    }

  } // End object BFOps



/** BFOrder provides the orderings to be used in joins.
 *
 *  none         - no ordering needed
 *  distinct     - eliminate consecutive duplicates
 *  sort         - ordering based on loci
 *  sortDistinct - ordering based on loci, and eliminate duplicate loci.
 *  plus         - eliminate negative strand
 *  minus        - eliminate positive strand
 *  flat         - merge overlapping loci
 */

  val BFOrder = BedFileOps.BFOrder
  
      

/** BFOut provides output formatting to be used in joins.
 *
 *  left(u, v)      - output u
 *  right(u, v)     - output v
 *  intersect(u, v) - output the intersection of u's and v's loci. 
 *  both(u, v)      - output u after copying v's locus into u's metadata
 *  cat(u, v)       - output the concatenation of u's and v's loci,
 *                    and both u's and v's metadata.   
 */

  val BFOut = BedFileOps.BFOut




/** BFCover provides the standard constraints to be used in coverR.
 *
 *  overlap(y, x)      - y.overlap(1)(x)
 *  overlapStart(y, x) - y.overlap(0)(x.start) 
 *  atleast(n), atmost(n), between(n, m), exactly(m)
 *  mdUpstream(n), mdDownstream(n), md(n)
 */

  val BFCover = BedFileOps.BFCover




/** Functions to facilitate more readable syntax
 *  when using the above to express queries.
 */

  object implicits {

    import scala.language.implicitConversions

    implicit def metaR[A](k: String) = (r: Bed) => r[A](k)
    // Get metadata k of a region

    implicit def aggrR[A](a: Aggr[Bed, A]) = (b: Iterator[Bed]) => a(b)
    // Compute an aggregate function on a track.

  }


} // End BedFileOps


