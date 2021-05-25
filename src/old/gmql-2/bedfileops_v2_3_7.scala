
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
 * 5 October 2020
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

    def partitionbyR(aggr: (String, Aggr[Bed, Double])*): BedFile = 
      BFOps.partitionbyR(aggr: _*)(us)

    def partitionbyR(grp: String, aggr: (String, Aggr[Bed, Double])*): BedFile =
      BFOps.partitionbyR(grp, aggr: _*)(us)

    def mapR
      (that: BedFile)
      (aggr: (String, Aggr[Bed, Double])*)
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

    def mapCoverR
      (that: BedFile)
      (aggr: (String, Aggr[Bed,Double])*)
      (implicit
         overlap: (Bed, Bed) => Boolean = 
           BFCover.overlapStart,
         screen: (Bed, Vector[Bed]) => Option[Bed] =
           BFCover.coverGen(BFCover.atleast(2)))
    : BedFile = BFOps.mapCoverR(aggr: _*)(overlap, screen)(us, that)

    def coverR
      (that: BedFile*)
      (minmax: Int => Boolean,
       aggr: (String, Aggr[Bed,Double])*)
    : BedFile = BFOps.coverR(minmax, aggr: _*)(us +: that)

    def flatR
      (that: BedFile*)
      (minmax: Int => Boolean,
       aggr: (String, Aggr[Bed,Double])*)
    : BedFile = BFOps.flatR(minmax, aggr: _*)(us +: that)

  }


  object BFOps {

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
        us.eiterator.map { r =>
          r.eraseMisc() ++ (proj.map { kf => val (k, f) = kf; (k, f(r))} : _*)
        }
      }


/** extendR(k1 -> f1, .., kn -> fn)(us)
 *
 *  SELECT u.*, k1 as f1(u), .., kn as fn(u) FROM us u
 */

    def extendR(proj: (String, Bed => Any)*)(us: BedFile): BedFile =
      BedFile.transientBedFile {
        us.eiterator.map { r => 
          r ++ (proj.map { kf => val (k, f) = kf; (k, f(r))} : _*)
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
      (aggr: (String, Aggr[Bed, Double])*)
      (us: BedFile)
    : BedFile = {
      //
      // Requires BedFile to be sorted on loci.
      //
      val f = aggr.combined
      val l = (u: Bed) => (u.chrom, u.chromStart, u.chromEnd)
      BedFile.transientBedFile {
        for ((k, v) <- us.partitionby(l)(f); (ch, cs, ce) = k)
        yield Bed(ch, cs, ce) ++ (v.toSeq: _*)
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
       aggr: (String, Aggr[Bed, Double])*)
      (us: BedFile)
    : BedFile = {
      //
      // Requires BedFile to be sorted on loci.
      //
      import AggrCollections.implicits._
      val f = aggr.combined
      val l = (u: Bed) => (u.chrom, u.chromStart, u.chromEnd)
      val g = (u: Bed) => u[Any](grp)
      BedFile.transientBedFile {
        for (
          (k, v) <- us.partitionby(l)(OpG.keep);
          (ch, cs, ce) = k;
          (gk, gv) <- v.groupby(g)(f)
        ) yield Bed(ch, cs, ce) ++ (("group" -> gk) +: (gv.toSeq): _*)
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

    def mapR(aggr: (String, Aggr[Bed, Double])*)(implicit ctr: String = "count")
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) =>
    { //
      // Requires both BedFiles to be sorted on loci.
      //
      import AggrCollections.implicits._
      val f = aggr.combined
      val bf = (y: Bed, x: Bed) => y isBefore x
      val cs = (y: Bed, x: Bed) => y.overlap(1)(x)
      val et = vs.eiterator
      val p = for (
                r <- us.eiterator;
                hs = et.syncedWith(r)(bf, cs).filter(_ sameStrand r);
                c = hs.length.toDouble;
                a = hs.flatAggregateBy(f)
              ) yield r ++ ((ctr -> c) +: (a.toSeq): _*)
      try BedFile.transientBedFile(p).serialized
      finally et.close()
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
      def overlap(x: Bed, y: Bed) = (exact match {
        case true  => x sameLocus y
        case false => x.overlap(1)(y)
      } ) && (x sameStrand y)

      diffs.isEmpty match {
        case true  => us
        case false =>
          import synchrony.iterators.FileCollections.EFile.merge
          val bf = (y: Bed, x: Bed) => y isBefore x
          val cs = (y: Bed, x: Bed) => y.overlap(1)(x)
          val merged = merge(diffs: _*).eiterator
          val tr = for (
                     r <- us.eiterator;
                     hs = merged.syncedWith(r)(bf, cs).filter(overlap(_, r));
                     if hs.length == 0
                   ) yield r
          try BedFile.transientBedFile(tr).serialized
          finally merged.close()
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
      val (ycs, ncs) = splitGenoPred(geno.toVector, Vector(), Vector()) match {
        case (y, n) => (if (y.length > 0) y else Vector(Locus.DLE(limit)), n)
      }
      val bf = Bed.isBefore _
      val cs = Locus.cond(ycs: _*) _
      val et = vs.eiterator
      val p = for (
                r <- us.eiterator;
                h <- et.syncedWith(r)(bf, cs);
                if screen(r, h) && Bed.cond(ncs: _*)(h, r);
                rh <- output(r, h)
              ) yield rh
      try ordering(p).serialized
      finally et.close()
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
      (aggr: (String, Aggr[Bed,Double])*)
      (implicit
        overlap: (Bed, Bed) => Boolean = 
          BFCover.overlapStart,
        screen: (Bed, Vector[Bed]) => Option[Bed] =
          BFCover.coverGen(BFCover.atleast(2)))
    : (BedFile, BedFile) => BedFile = (us: BedFile, vs: BedFile) => {
      //
      // Requires all BedFiles are sorted on loci, 
      // and within each BedFiles, regions are distinct;
      // i.e. no two regions in a BedFile have same loci and strand.
      //
      import AggrCollections.implicits._
      val f  = aggr.combined
      val rt = us.eiterator
      val et = vs.eiterator
      val bf = (y: Bed, x: Bed) => y isBefore x
      val cs = overlap
      val tr = for (
                 r <- rt;
                 hs = et.syncedWith(r)(bf, cs).filter(_ sameStrand r);
                 b <- screen(r, hs);
                 a  = hs.flatAggregateBy(f).toSeq;
                 nb  =  b ++ (a: _*)
               ) yield nb
        try BedFile.transientBedFile(tr).serialized
        finally { rt.close(); et.close() }
      }


    def coverR(
      minmax: Int => Boolean,
      aggr: (String, Aggr[Bed,Double])*
    ): Seq[BedFile] => BedFile = (vs: Seq[BedFile]) => vs.length match {
      //
      // Requires all BedFiles are sorted on loci, 
      // and within each BedFiles, regions are distinct;
      // i.e. no two regions in a BedFile have same loci and strand.
      //
      case 0 => 
        BedFile.inMemoryBedFile(Vector())
  
      case 1 =>
        BedFile.inMemoryBedFile(Vector())

      case _ =>
        import BFCover.{ overlapStart, coverGen}
        val merged = BedFile.merge(vs: _*)(onDisk = true)
        val starts = BedFile.transientBedFile(merged.eiterator.map(_.start))
        val ends   = BedFile.transientBedFile(merged.eiterator.map(_.end))
        val mrf    = BedFile.merge(starts, ends)
        val rf     = BFOrder.distinct(mrf.eiterator).serialized
        mapCoverR(aggr: _*)(overlapStart, coverGen(minmax))(rf, merged)
      }


      
    def flatR(
      minmax: Int => Boolean,
      aggr: (String, Aggr[Bed,Double])*
    ): Seq[BedFile] => BedFile = (vs: Seq[BedFile]) => vs.length match {
      //
      // Requires all BedFiles are sorted on loci, 
      // and within each BedFiles, regions are distinct;
      // i.e. no two regions in a BedFile have same loci and strand.
      //
      case 0 => 
        BedFile.inMemoryBedFile(Vector())
  
      case 1 =>
        BedFile.inMemoryBedFile(Vector())

      case _ =>
        import BFCover.{ overlapStart, coverFlat}
        val merged = BedFile.merge(vs: _*)(onDisk = true)
        val starts = BedFile.transientBedFile(merged.eiterator.map(_.start))
        val ends   = BedFile.transientBedFile(merged.eiterator.map(_.end))
        val mrf    = BedFile.merge(starts, ends)
        val rf     = BFOrder.distinct(mrf.eiterator).serialized
        mapCoverR(aggr: _*)(overlapStart, coverFlat(minmax))(rf, merged)
      }


      /** Actually, mapR and differenceR (and in fact, joinR as well)
       *  can be defined using mapCoverR straightforwardly; see below.
       *
       *  def mapR(aggr:(String,Aggr[Bed,Double])*)(implicit ctr:String="count")
       *  = (us: BedFile, vs: BedFile) => {
       *      mapCoverR
       *        (aggr: _*)
       *        (overlap = BFCover.overlap,
       *         screen  = (r, hs) => Some(r ++ (ctr -> hs.length.toDouble))) 
       *        (us, vs)
       *  }
       *
       *
       *  def differenceR(exact: Boolean = false)
       *  = (us: BedFile, diffs: Seq[BedFile]) => {
       *      val overlapExact = (x: Bed, hs: Vector[Bed]) => {
       *        val nhs = exact match {
       *          case false => hs
       *          case true  => hs.filter(y => exact && (x sameLocus y))
       *        }
       *        if (nhs.length == 0) Some(r) else None
       *      }
       *      mapCoverR()(BFCover.overlap, overlapExact)(us, vs)
       *  }
       */

  } // End BFOps




/** aggrCombineN is an auxiliary function for converting
 *  aggregate functions that iterate on bedFile independently
 *  into a single aggregate function that iterate on bedFile
 *  only once.
 */

    implicit class AggrCombineN[A](aggr: Seq[(String, Aggr[A, Double])]) {

      def combined: Aggr[A, Array[(String, Double)]] = {
        val aggrs = aggr.map { ka => 
          val (k, a) = ka
          a |> ((x: Double) => (k, x))
        }
        def mix(a: Array[(String, Double)]) = a
        combineN[A, (String,Double), Array[(String,Double)]](aggrs: _*)(mix)
      }
    }




/** BFOrder provides the orderings to be used in joins.
 *
 *  none         - no ordering needed
 *  distinct     - eliminate consecutive duplicates
 *  sort         - ordering based on loci
 *  sortDistinct - ordering based on loci, and eliminate duplicate loci.
 */

  object BFOrder {

    def none(it: Iterator[Bed]): BedFile = 
      BedFile.transientBedFile(it)
    
    def distinct(it: Iterator[Bed]): BedFile = {
      // Requires it is sorted on loci.
      var tmp: Option[Bed] = None
      val p = for (x <- it; if (tmp != Some(x)); _ = tmp = Some(x)) yield x
      BedFile.transientBedFile(p)
    }
      
    def sort(it: Iterator[Bed]): BedFile = BedFile.transientBedFile(it).sorted

    def sortDistinct(it: Iterator[Bed]): BedFile = distinct(sort(it).eiterator)
  }
  
      

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

    // For use with join

    val left = (x: Bed, y: Bed) => Some(x)

    val right = (x: Bed, y: Bed) => Some(y)

    def intersectGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => (x.locus intersect y.locus) match {
        case None    => None
        case Some(l) => Some(
          Bed(l)
          .overwriteMisc(x.misc)
          .overwriteMisc(y.renameMisc(f).misc))
    }

    val intersect = intersectGen()

    def bothGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => Some(
         x.overwriteMisc(Map(
           f("chrom")      -> y.chrom,
           f("chromStart") -> y.chromStart,
           f("chromEnd")   -> y.chromEnd)))

    val both = bothGen()

    def catGen(f: String => String = (p => "r." ++ p)) =
      (x: Bed, y: Bed) => Some(
         Bed(Locus(
           x.chrom, 
           x.chromStart min y.chromStart,
           x.chromEnd max y.chromEnd))
         .overwriteMisc(x.misc)
         .overwriteMisc(y.renameMisc(f).misc))

    val cat = catGen()
  }



/** BFCover provides the standard constraints to be used in coverR.
 */

  object BFCover {

    def overlap = (y: Bed, x: Bed) => y.overlap(1)(x)

    def overlapStart = (y: Bed, x: Bed) => y.overlap(0)(x.start)

    def coverGen(f: Int => Boolean) =
    (r: Bed, hs: Vector[Bed]) => hs.length match {
      case n if f(n) =>
        val cs = r.chromStart;
        val ps = hs.map(_.chromStart).min
        val ce = hs.map(_.chromEnd).min
        val pe = hs.map(_.chromEnd).max
        val cn = hs.length
        val j  = (ce - cs)/(pe - ps)
        Some(r.++("chromEnd" -> ce, "accIndex" -> cn, "jaccardIndex" -> j))
      case _ =>
        None
    }

    def coverFlat(f: Int => Boolean) =
    (r: Bed, hs: Vector[Bed]) => hs.length match {
      case n if f(n) =>
        val cs = r.chromStart;
        val ps = hs.map(_.chromStart).min
        val ce = hs.map(_.chromEnd).min
        val pe = hs.map(_.chromEnd).max
        val cn = hs.length
        val j  = (ce - cs)/(pe - ps)
        Some(r.++("chromStart" -> ps, "chromEnd" -> pe,
                   "accIndex" -> cn, "jaccardIndex" -> j))
      case _ =>
        None
    }

    def atleast(min: Int) = (n: Int) => n >= min 

    def atmost(max: Int) = (n: Int) => n <= max

    def between(min: Int, max: Int) = (n: Int) => min <= n && n <= max 

    def exactly(m: Int) = (n: Int)  => n == m
  }



/** Functions to facilitate more readable syntax
 *  when using the above to express queries.
 */

  import scala.language.implicitConversions

  implicit def metaR[A](k: String) = (r: Bed) => r[A](k)
  // Get metadata k of a region

  implicit def aggrR(a: Aggr[Bed, Double]) = (b: Iterator[Bed]) => a(b)
  // Compute an aggregate function on a track.

} // End BedFileOps


