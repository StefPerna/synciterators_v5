
package synchrony.genomeannot


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
 * 28 October 2020
 */


object BedFileOps {

  import synchrony.genomeannot.BedWrapper
  import synchrony.genomeannot.GenomeAnnot
  import synchrony.iterators.SyncCollections
  import synchrony.iterators.AggrCollections
  import synchrony.iterators.FileCollections.EFile
  import synchrony.programming.Sri
  import synchrony.programming.Sri._

  val Locus = GenomeAnnot.GenomeLocus
  val Bed = BedWrapper.SimpleBedEntry
  val BedFile = BedWrapper.BedFile
  val OpG = AggrCollections.OpG
  val EIterator = SyncCollections.EIterator
  
  type Bed = BedWrapper.SimpleBedEntry
  type BedFile = BedWrapper.BedFile
  type BedEIterator = BedWrapper.BedEIterator
  type LocusLike = GenomeAnnot.LocusLike
  type LocusPred = Locus.LocusPred
  type EIterator[A] = SyncCollections.EIterator[A]

  // Type of aggregate function C => F
  type Aggr[C, F] = Sri[C, F] 



  implicit class BFOps(us: BedFile) {

    def selectR(pred: Bed => Boolean): BedFile = 
      BFOps.selectR(pred)(us)

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


/** Auxiliary codes to optimize updating of metadata in Bed file.
 *  The required fields (chrom, chromStart, chromEnd, name, score,
 *  strand) requires special treatment if update involves them. 
 *
 *  mkOverwriteMisc selects the update function to compile into
 *  depending on the fields (p).
 */

    def mkOverwriteMisc[A](p: Seq[(String, A)]) = {
      val conflict = p.exists { case (k, a) =>
        k == "chrom" || k == "chromStart" || k == "chromEnd" ||
        k == "name"  || k == "score"      || k == "strand"
      }
      (p.length == 0, conflict) match {
        case (true, _) => (r: Bed, m: Map[String, Any]) => r
        case (_, true)  => (r: Bed, m: Map[String, Any]) => r.overwriteMisc(m) 
        case _ => (r: Bed, m: Map[String, Any]) => r.simpleOverwriteMisc(m) 
      }
    }


/** selectR(f)(us) 
 *
 *  SELECT u.* FROM us u WHERE f(u) 
 */

    def selectR(pred: Bed => Boolean)(us: BedFile): BedFile = 
      us.filtered(r => pred(r))


/** projectR(k1 -> f1, .., kn -> fn)(us) 
 *
 *  SELECT k1 as f1(u), .., kn as fn(u) FROM us u
 */

    def projectR(proj: (String, Bed => Any)*)(us: BedFile): BedFile =
      BedFile.transientBedFile {
        val overwrite = mkOverwriteMisc(proj) 
        us.eiterator.map { r =>
          val ps =  proj map { kf => val (k, f) = kf; (k, f(r)) }
          overwrite(r.eraseMisc(), ps.toMap) 
        }
      }


/** extendR(k1 -> f1, .., kn -> fn)(us)
 *
 *  SELECT u.*, k1 as f1(u), .., kn as fn(u) FROM us u
 */

    def extendR(proj: (String, Bed => Any)*)(us: BedFile): BedFile =
      BedFile.transientBedFile {
        val overwrite = mkOverwriteMisc(proj) 
        us.eiterator.map { r => 
          val ps =  proj map { kf => val (k, f) = kf; (k, f(r)) }
          overwrite(r, ps.toMap)
        }
      }



/** partitionbyR(k1 -> a1, .., kn -> an)(us)
 *
 *  SELECT u.chrom, u.chromStart, u.chromEnd,
 *         k1 as A1, .., kn  as An
 *  FROM us u
 *  GROUPBY u.chrom u.chromStart, u.chromEnd
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group.
 */

    def partitionbyR
      (aggr: (String, Aggr[Bed, Any])*)
      (us: BedFile)
    : BedFile = {
      //
      // Requires BedFile to be sorted on loci.
      //
      val f = aggr.combined
      val l = (u: Bed) => (u.chrom, u.chromStart, u.chromEnd)
      val overwrite = mkOverwriteMisc(aggr) 
      BedFile.transientBedFile {
        for ((k, v) <- us.partitionby(l)(f); (ch, cs, ce) = k)
        yield overwrite(Bed(ch, cs, ce), v.toMap)
      }
    }


/** partitionbyR(g, k1 -> a1, .., kn -> an)(us)
 *
 *  SELECT u.chrom, u.chromStart, u.chromEnd, u.g, k1 as A1, .., kn  as An
 *  FROM us u
 *  GROUPBY u.chrom u.chromStart, u.chromEnd, u.g
 *  WITH A1 = a1 applied to the group, ..,
 *       An = an applied to the group.
 */

    def partitionbyR
      (grp: String, 
       aggr: (String, Aggr[Bed, Any])*)
      (us: BedFile)
    : BedFile = {
      //
      // Requires BedFile to be sorted on loci.
      //
      import AggrCollections.implicits._
      val f = aggr.combined
      val l = (u: Bed) => (u.chrom, u.chromStart, u.chromEnd)
      val g = (u: Bed) => u[Any](grp)
      val overwrite = mkOverwriteMisc(aggr) 
      BedFile.transientBedFile {
        for (
          (k, v) <- us.partitionby(l)(OpG.keep);
          (ch, cs, ce) = k;
          (gk, gv) <- v.groupby(g)(f)
        ) yield overwrite(Bed(ch, cs, ce), gv.toMap + ("group" -> gk))
      }
    }


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

    def mapR(aggr: (String, Aggr[Bed, Any])*)(implicit ctr: String = "count")
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) =>
    { //
      // Requires both BedFiles to be sorted on loci.
      //
      import synchrony.iterators.SyncCollections.SynchroEIterator
      import AggrCollections.implicits._
      val aggrWithCtr = (ctr, OpG.count[Bed]) +: aggr
      val overwrite = mkOverwriteMisc(aggrWithCtr) 
      val f = aggrWithCtr.combined
      val tr = SynchroEIterator.map(
        isBefore = Bed.isBefore _,
        canSee   = Locus.cond(Locus.Overlap(1)) _,
        screen   = Bed.cond() _,
        convex   = false,
        early    = false,
        extrack  = vs.eiterator,
        lmtrack  = us.eiterator) {
         (r: Bed, hs: Vector[Bed]) => overwrite(r, hs.flatAggregateBy(f).toMap)
        } 
      BedFile.transientBedFile(tr)
    }



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

    def differenceR(implicit exact: Boolean = false)
    : (BedFile, Seq[BedFile]) => BedFile = (us: BedFile, diffs: Seq[BedFile]) =>
    { //
      // Requires both bedFile and diffs to be sorted on loci.
      //
      def overlap(x: LocusLike, y: LocusLike) = exact match {
        case true  => x sameLocus y
        case false => x.overlap(1)(y)
      } 

      diffs.isEmpty match {
        case true  => us
        case false =>
          import synchrony.iterators.FileCollections.EFile.merge
          import synchrony.iterators.SyncCollections.SynchroEIterator
          val tr = SynchroEIterator.map(
            isBefore = Bed.isBefore _,
            canSee   = Locus.cond(Locus.Overlap(1)) _,
            screen   = Bed.cond(Locus.GenPred(overlap)) _,
            grpscreen= (r: Bed, hs: Vector[Bed]) => hs.isEmpty,
            n        = 1,
            convex   = false,
            early    = false,
            extrack  = merge(diffs: _*).eiterator,
            lmtrack  = us.eiterator) {
              (r: Bed, hs: Vector[Bed]) => r
            }
          BedFile.transientBedFile(tr)
      }   
    }



/** splitGenoPred splits a list (ps) of genometric predicates
 *  (i.e., predicates on a pair of loci of genomic regions)
 *  into those which are convex (ycs) and not convex (ncs).
 *  The convex ones can be made into a canSee predicate for
 *  Synchrony iterators.
 */

    private def splitGenoPred(
      ps:  Vector[LocusPred],
      ycs: Vector[LocusPred],
      ncs: Vector[LocusPred]
    ): (Vector[LocusPred], Vector[LocusPred]) = {
      // split ps into those (ycs) convex wrt isBefore
      // and those which are not (ncs).
      //
      if (ps.isEmpty) (ycs, ncs) else ps.head match {
        case p: Locus.DLE => splitGenoPred(ps.tail, ycs :+ p, ncs)
        case p: Locus.DL  => splitGenoPred(ps.tail, ycs :+ p, ncs)
        case p            => splitGenoPred(ps.tail, ycs, ncs :+ p)
      }
    }


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
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) =>
    { //
      // Requires both bedfile and that to be sorted on loci.
      //
      import synchrony.iterators.SyncCollections.SynchroEIterator
      val (ycs, ncs) = splitGenoPred(geno.toVector, Vector(), Vector()) match {
        case (y, n) => (if (y.length > 0) y else Vector(Locus.DLE(limit)), n)
      }
      def check(t: Bed, r: Bed) = screen(r, t) && Bed.cond(ncs: _*)(t, r)
      val tr = SynchroEIterator.ext(
        isBefore  = Bed.isBefore _,
        canSee    = Locus.cond(ycs: _*) _,
        screen    = check _,
        grpscreen = (r: Bed, hs: Vector[Bed]) => !hs.isEmpty,
        convex    = false,
        early     = true,
        extrack   = vs.eiterator,
        lmtrack   = us.eiterator) {
          case (r, hs) => for (h <- hs; o <- output(r, h)) yield o
        }
      ordering(tr)
    }


    def iNearestR
      (geno: LocusPred*)
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = limit)
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) =>
    { //
      // Requires both bedfile and that to be sorted on loci.
      //
      import synchrony.iterators.SyncCollections.SynchroEIterator
      val (ycs, ncs) = splitGenoPred(geno.toVector, Vector(), Vector()) match {
        case (y, n) => (if (y.length > 0) y else Vector(Locus.DLE(limit)), n)
      }
      def check(t: Bed, r: Bed) = screen(r, t) && Bed.cond(ncs: _*)(t, r)
      val tr = SynchroEIterator.ext(
        isBefore  = Bed.isBefore _,
        canSee    = Locus.cond(ycs: _*) _,
        screen    = check _,
        grpscreen = (r: Bed, hs: Vector[Bed]) => !hs.isEmpty,
        convex    = false,
        early     = true,
        extrack   = vs.eiterator,
        lmtrack   = us.eiterator) {
          case (r, hs) => for (h <- nearest(r, hs); o <- output(r, h)) yield o
        }
      ordering(tr)
    }


    def oNearestR
      (nearest: (Bed, Vector[Bed]) => Vector[Bed] = BFCover.md(1))
      (geno: LocusPred*)
      (implicit
         output: (Bed, Bed) => Option[Bed] = BFOut.both, 
         screen: (Bed, Bed) => Boolean = (u: Bed, v: Bed) => true,
         ordering: Iterator[Bed] => BedFile = BFOrder.none,
         limit: Int = limit)
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) =>
    { //
      // Requires both bedfile and that to be sorted on loci.
      //
      import synchrony.iterators.SyncCollections.SynchroEIterator
      val (ycs, ncs) = splitGenoPred(geno.toVector, Vector(), Vector()) match {
        case (y, n) => (if (y.length > 0) y else Vector(Locus.DLE(limit)), n)
      }
      val tr = SynchroEIterator.ext(
        isBefore  = Bed.isBefore _,
        canSee    = Locus.cond(ycs: _*) _,
        screen    = Bed.cond() _,
        grpscreen = (r: Bed, hs: Vector[Bed]) => !hs.isEmpty,
        convex    = false,
        early     = true,
        extrack   = vs.eiterator,
        lmtrack   = us.eiterator) {
          case (r, hs) => 
            for (
              h <- nearest(r, hs);
              if screen(r, h) && Bed.cond(ncs: _*)(h, r);
              o <- output(r, h)
            ) yield o
        }
      ordering(tr)
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
    : Seq[BedFile] => BedFile = (vs: Seq[BedFile]) => vs.length match {
      //
      // Requires all BedFiles are sorted on loci; and
      // overlap is convex or antimonotonic.
      //
      case 0 =>
        BedFile.inMemoryBedFile(Vector())

      case _ =>
        import AggrCollections.implicits._
        import synchrony.iterators.SyncCollections.SynchroEIterator

        val merged = vs.length match {
          case 1 => vs.head.stored
          case _ => BedFile.merge(vs: _*).stored
        }
        def mm(it: EIterator[Bed]) = stranded match {
          case true  => BedFile.transientBedFile(it)
          case false => BFOrder.nullStrand(it)
        }
        val starts = mm(merged.eiterator.map(_.start))
        val ends   = mm(merged.eiterator.map(_.end)).stored
        val rf     = BFOrder.distinct(
                       starts.mergedWith(ends.sortedIfNeeded).eiterator
                     ).stored
        starts.destruct()
        ends.destruct()
   
        val tr = SynchroEIterator.ext(
          isBefore = Bed.isBefore _,
          canSee   = overlap,
          screen   = Bed.cond(Locus.EndAfter) _,
          convex   = false,
          early    = false,
          extrack  = merged.eiterator,
          lmtrack  = rf.eiterator) {
            (r, hs) => screen(r, hs).toVector
          }

        val coverRes = stranded match {
          case true  => BFOrder.withStrand(coverOp)(tr).stored
          case false => coverOp(tr).stored
        }
        tr.close()
        rf.destruct()

        // Do the aggregate functions only if there are some.
        def aggrRes = mapR(aggr: _*)("accIndex")(coverRes, merged)
        if (aggr.length == 0) coverRes else aggrRes
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




/** aggrCombineN is an auxiliary function for converting
 *  aggregate functions that iterate on bedFile independently
 *  into a single aggregate function that iterate on bedFile
 *  only once.
 */

    implicit class AggrCombineN[A, B](aggr: Seq[(String, Aggr[A, B])]) {

      def combined: Aggr[A, Array[(String, B)]] = {
        val aggrs = aggr.map { ka => 
          val (k, a) = ka
          a |> ((x: B) => (k, x))
        }
        def mix(a: Array[(String, B)]) = a
        combineN[A, (String,B), Array[(String,B)]](aggrs: _*)(mix)
      }
    }




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

  object BFOrder {

    // For use with joinR

    def none(it: Iterator[Bed]): BedFile = BedFile.transientBedFile(it)
    
    def distinct(it: Iterator[Bed]): BedFile = BedFile.transientBedFile {
      // Requires it is sorted on loci.
      var tmp: Option[Bed] = None
      for (x <- it; if (tmp != Some(x)); _ = tmp = Some(x)) yield x
    }
      
    def sort(it: Iterator[Bed]): BedFile = BedFile.transientBedFile(it).sorted

    def sortDistinct(it: Iterator[Bed]): BedFile = distinct(sort(it).eiterator)


    // For use with coverR, histogramR, flatR, summitR

    def plus(it: Iterator[Bed]): BedFile = distinct {
      for (
        x <- it;
        if x.strand != "-"
      ) yield if (x.strand == "+") x else x ++ ("strand" -> "+")
    }

    def minus(it: Iterator[Bed]): BedFile = distinct {
      for (
        x <- it;
        if x.strand != "+"
      ) yield if (x.strand == "-") x else x ++ ("strand" -> "-")
    }

    def nullStrand(it: Iterator[Bed]): BedFile = distinct {
      for (x <- it)
      yield if (x.strand == ".") x else x ++ ("strand" -> ".")
    }


    def withStrand(f: Iterator[Bed] => BedFile) = {
    (it: Iterator[Bed]) =>
      val bf = BedFile.transientBedFile(it).stored
      val plusStrand = f(plus(bf.eiterator).eiterator)
      val minusStrand = f(minus(bf.eiterator).eiterator)
      try plusStrand.mergedWith(minusStrand).serialized
      finally { bf.destruct(); plusStrand.destruct(); minusStrand.destruct() }
    } 


    def histo(minmax: Int => Boolean) = { (it: Iterator[Bed]) =>
      val tr = new EIterator[Bed] {
        // val eit = EIterator[Bed](it).filter(r => minmax(r[Int]("htIndex")))
        val eit = EIterator[Bed](it)
        def mySeek(): Unit = {
          def minmaxNext = eit.peekahead(1) match {
            case Some(nxt) => minmax(nxt[Int]("htIndex"))
            case None      => false
          }
          while (eit.hasNext && !minmaxNext) { eit.next() }
        }
        override def myClose(): Unit = eit.close()
        override def myHasNext: Boolean = { mySeek(); eit.hasNext }
        override def myNext(): Bed = {
          mySeek()
          var cur = eit.next()
          val cnt = cur[Int]("htIndex")
          def canMerge(x: Bed) = cur.overlap(0)(x) && cnt == x[Int]("htIndex")
          while (eit.hasNext && canMerge(eit.peekahead(1).get)) {
            val nxt = eit.next()
            cur = Bed(cur.chrom, cur.chromStart, nxt.chromEnd,
                      cur.strand, cur.name, cur.score, cur.misc)
          }
          eit.peekahead(1) match {
            case None => cur
            case Some(nxt) => cur.overlap(0)(nxt) match {
              case true  => Bed(cur.chrom, cur.chromStart, nxt.chromStart, 
                                cur.strand, cur.name, cur.score, cur.misc)
              case false => cur
            }
          }
        }
      }
      BedFile.transientBedFile(tr)
    }


    def summit(minmax: Int => Boolean) = { (it: Iterator[Bed]) =>
      val eit = histo(minmax)(it).eiterator
      var up  = true
      def isSummit(cur: Bed) = eit.peekahead(1) match {
        case None      => { val tmp = up; up = true; tmp }
        case Some(nxt) => cur.overlap(0)(nxt) match {
          case false => { val tmp = up; up = true; tmp }
          case true  => 
            (cur[Int]("htIndex") > nxt[Int]("htIndex")) match {
               case false => { up = true; false }
               case true  => { val tmp = up; up = false; tmp }
          }
        }
      }
      val tr  = for (cur <- eit; if isSummit(cur)) yield cur
      BedFile.transientBedFile(tr)
    }


    def cover(minmax: Int => Boolean) = { (it: Iterator[Bed]) =>
      val tr = new EIterator[Bed] {
        val eit = histo(minmax)(it).eiterator
        override def myClose(): Unit = eit.close()
        override def myHasNext: Boolean = eit.hasNext
        override def myNext(): Bed = {
          var acc = eit.next()
          while (eit.hasNext && acc.overlap(0)(eit.peekahead(1).get)) {
            val nxt = eit.next()
            acc = Bed(acc.chrom, acc.chromStart, nxt.chromEnd,
                      acc.strand, acc.name, acc.score, acc.misc)
          }
          acc
        }
      }
      BedFile.transientBedFile(tr)
    }

 
    def flat(minmax: Int => Boolean) = { (it: Iterator[Bed]) =>
      val tr = new EIterator[Bed] {
        val eit = EIterator[Bed](it).filter(b => minmax(b[Int]("htIndex")))
        override def myClose(): Unit = eit.close()
        override def myHasNext: Boolean = eit.hasNext
        override def myNext(): Bed = {
          var acc = eit.next()
          while (eit.hasNext && acc.overlap(0)(eit.peekahead(1).get)) {
            val nxt = eit.next()
            acc = Bed(acc.chrom, acc.chromStart min nxt.chromStart,
                      acc.chromEnd max nxt.chromEnd, acc.strand, 
                      acc.name, acc.score, acc.misc)
          }
          acc
        }
      }
      BedFile.transientBedFile(tr)
    }


    def complement =  { (it: Iterator[Bed]) =>
      val eit = EIterator[Bed](it)
      val tr  = for (
                  cur <- eit;
                  nxt <- eit.peekahead(1);
                  if cur endBefore nxt
                ) yield Bed(cur.chrom, cur.chromEnd, nxt.chromStart, cur.strand)
      BedFile.transientBedFile(tr)
    }

  } // End object BFOrder
  
      

/** BFOut provides output formatting to be used in joins.
 *
 *  left(u, v)      - output u
 *  right(u, v)     - output v
 *  intersect(u, v) - output the intersection of u's and v's loci. 
 *  both(u, v)      - output u after copying v's locus into u's metadata
 *  cat(u, v)       - output the concatenation of u's and v's loci,
 *                    and both u's and v's metadata.   
 */

  object BFOut {

    // For use with joinR

    val left = (x: Bed, y: Bed) => Some(x)

    val right = (x: Bed, y: Bed) => Some(y)

    def intersectGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => (x.locus intersect y.locus) match {
        case None    => None
        case Some(l) => Some(
          Bed(
            l.chrom, l.chromStart, l.chromEnd,
            x.strand, x.name, x.score, x.misc)
          .simpleOverwriteMisc(y.renameMisc(f).misc))
    }

    val intersect = intersectGen()

    def bothGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => Some(
         x.simpleOverwriteMisc(Map(
           f("chrom")      -> y.chrom,
           f("chromStart") -> y.chromStart,
           f("chromEnd")   -> y.chromEnd)))

    val both = bothGen()

    def catGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => Some(
         Bed(
           x.chrom, x.chromStart min y.chromStart, x.chromEnd max y.chromEnd,
           x.strand, x.name, x.score, x.misc)
         .simpleOverwriteMisc(y.renameMisc(f).misc))

    val cat = catGen()

  }



/** BFCover provides the standard constraints to be used in coverR.
 */

  object BFCover {

    // For use with coverR

    def overlap = (y: Bed, x: Bed) => y.overlap(1)(x)

    def overlapStart = (y: Bed, x: Bed) => y.overlap(0)(x.start)

    def coverGenMin = (r: Bed, hs: Vector[Bed]) => hs.isEmpty match {
      case true  => Vector()
      case false => 
        val (ch, cs, ce) = (r.chrom, r.chromStart, hs.map(_.chromEnd).min)
        val (sr, cn)     = (r.strand, hs.length)
        Vector(Bed(ch, cs, ce, sr, "", 0, Map("htIndex" -> cn)))
    }

    def coverGenMax = (r: Bed, hs: Vector[Bed]) => hs.isEmpty match {
      case true  => Vector()
      case false => 
        val ch       = r.chrom
        val (cs, ce) = (hs.map(_.chromStart).min, hs.map(_.chromEnd).max)
        val (sr, cn) = (r.strand, hs.length)
        Vector(Bed(ch, cs, ce, sr, "", 0, Map("htIndex" -> cn)))
    }

    def atleast(min: Int) = (n: Int) => n >= min 

    def atmost(max: Int) = (n: Int) => n <= max

    def between(min: Int, max: Int) = (n: Int) => min <= n && n <= max 

    def exactly(m: Int) = (n: Int)  => n == m


    // For use with iNearestR and oNearestR

    def mdUpstream(n: Int) = (x: Bed, hs: Vector[Bed]) => { 
      val tmp = hs.filter(_ endBefore x)
      tmp.drop(tmp.length - n)
    }

    def mdDownstream(n: Int) = (x: Bed, hs: Vector[Bed]) => 
       hs.filter(_ startAfter x).take(n)
    
    def md(n: Int) = (x: Bed, hs: Vector[Bed]) => {
      hs.filter(h => (h endBefore x) || (h startAfter x))
        .sortWith((u, v) => u.distFrom(x) < v.distFrom(x))
        .take(n)
    }
  }



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


