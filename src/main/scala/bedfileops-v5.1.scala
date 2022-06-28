
package gmql

/** Version 5, to be used with gmql.BEDModel Version 5.
 *
 *  Wong Limsoon
 *  30 Jan 2022
 */


object BEDFileOps {

/** Organization:
 *
 *  object BFOs
 *  // Contains: selectR, projectR, extendR, partitionbyR, 
 *  //           differenceR, mapR, joinR, iNearestR, oNearestR, 
 *  //           coverR, summitR, histoR, flatR, complementR
 *
 *  object BFOrder
 *  // Contains: NONE, DISTINCT, SORT, SORTDISTINCT,
 *               PLUS, MINUS, NULLSTRAND, COMPLEMENT, 
 *               HISTO(minmax), SUMMIT(minmax), COVER(minmax), FLAT(minmax)
 *
 *  object BFOut
 *  // Contains: LEFT, RIGHT, INT, BOTH, CAT, intersectf, bothf, catf
 *
 *  object BFCover
 *  // Contains: ATLEAST(n), ATMOST(n), BETWEEN(n,m), EXACTLY(n)
 *               coverGenMin, coveerGenMax
 *
 *  object Genometric
 *  // Contains: UPSTREAM(k), DOWNSTREAM(k), MD(k),
 *     antimono: BEFORE, SAMECHROM, SAMELOCUS, SAMESTART, 
 *               STARTBEFORE, OVERLAPSTART, OVERLAP(n), NEAR(n),
 *               EQ, LT, LTEQ, DL(n), DLEQ(n), SZPERCENT(n),
 *      nonanti: STARTAFTER, ENDBEFORE, ENDAFTER, SAMEEND,
 *               OVERLAPEND, FAR(n), INSIDE, ENCLOSE, TOUCH, OUTSIDE,
 *               GT, GTEQ, DG(n), DGEQ(n)
 *
 *  object BFAggr
 *  // Contains: COUNT, SUMInt(f), SMALLESTInt(f), BIGGESTInt(f), 
 *               AVERAGE, SUM, SMALLEST(f), BIGGEST(f), 
 *               MINIMIZE(f), MAXIMIZE(f)
 */



/** Synchrony GMQL
 *
 *  This package provides operations on BED files. The provided operations
 *  are based on those of GMQL, but I extracted here only those parts 
 *  pertaining to BED files, discarding those parts pertaining to Samples.
 *  GMQL mixes these two parts in a non-orthogonal way, which makes it less
 *  convenient to be used as a programming library on BED files (without
 *  the baggage on samples.)
 *
 *  Conceptually, a BED file can be regarded as a relational table,
 *
 *    BedFile[Bed(chrom, start, end, name, score, strand, r1, ..., rn)] 
 *               
 *  where r1, .., rn are the metadata on a region.  The BED file can be 
 *  thought of as a "file-based vector" which transparently materializes
 *  some rows in the underlying BED file into memory when these rows are
 *  needed in a computation.
 *
 *  The operations on BED files provided by GMQL are basically relational
 *  algebra operations (SELECT, PROJECT, JOIN, GROUPBY), as well as
 *  operations with specialized genomic meaning (MAP, DIFFERENCE, and
 *  COVER) on the BED file. 
 *
 *  MAP is equivalent to a JOIN+GROUPBY+aggregation function query: 
 *  it compares two BED files (call these the landmark and experiment
 *  tracks), grouping the regions/BED entries in the experiment track to
 *  regions/BED entries which their loci overlap with in the landmark
 *  track, applying some aggregate functions on each resulting group,
 *  and finally producing for each region in the landmark track, the
 *  corresponding aggregate function results as the output.
 *
 *  DIFFERENCE is similar to MAP, but the landmark track is compared 
 *  to multiple experiment tracks. The output is those regions in the
 *  landmark track that overlap with no region in all the experiment
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
 *  relational query equivalent. In its simplest form COVER(n,m), applied
 *  to a set of tracks, outputs (sub)regions that overlap with at least
 *  n and atmost m of the tracks.
 */


  import scala.language.implicitConversions
  import scala.language.existentials
  import gmql.GenomeLocus._
  import gmql.BEDModel.BEDEntry._
  import gmql.BEDModel.BEDFile._
  import dbmodel.DBModel._
  import dbmodel.DBModel.Join._
  import dbmodel.DBModel.CloseableIterator._
  import dbmodel.DBModel.Predicates._
  import dbmodel.DBModel.OpG.{ Aggr, AGGR }

  
  type Bool             = Boolean
  type ANTIMONOTONIC    = (Locus,Locus) => Bool
  type NONANTIMONOTONIC = (Bed,Bed) => Bool


  var STORE = false   // If true, always BFOps always serializes BED files.



  /** Create [[BFOps]] as an implicit class to package GMQL functions
   *  on BED files.
   */


  implicit class BFOps(us: BEDFILE)
  {
    def selectR(pred: Bed => Bool): BEDFILE = 
      BFOps.selectR(pred)(us)


    def projectR(proj: (String, Bed => Any)*): BEDFILE = 
      BFOps.projectR(proj: _*)(us)


    def extendR(proj: (String, Bed => Any)*): BEDFILE = 
      BFOps.extendR(proj: _*)(us)


    def partitionbyR(aggr: (String, AGGR[Bed,Any])*): BEDFILE = 
      BFOps.partitionbyR(aggr: _*)(us)


    def partitionbyR(grp: String, aggr: (String, AGGR[Bed,Any])*): BEDFILE =
      BFOps.partitionbyR(grp, aggr: _*)(us)


    def mapR
      (vs: BEDFILE, aggr: (String, AGGR[Bed,Any])*)
      (implicit ctr: String = "count"): BEDFILE =
    {
      BFOps.mapR(aggr: _*)(ctr)(us, vs)
    }


    def differenceR(vs: BEDFILE*)(implicit exact: Bool = false): BEDFILE =
      BFOps.differenceR(exact)(us, vs)


    def joinR
      (vs: BEDFILE, 
       antimonotonic: ANTIMONOTONIC, 
       nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE): BEDFILE =
    {
      BFOps.joinR(antimonotonic, nonantimonotonic: _*)(output, ordering)(us, vs)
    }


    def iNearestR
      (vs: BEDFILE,
       antimonotonic: ANTIMONOTONIC, 
       nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         nearest:  (Bed, List[Bed]) => List[Bed] = Genometric.MD(1),
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE) : BEDFILE =
    { 
      BFOps.iNearestR(
        antimonotonic, 
        nonantimonotonic: _*)(nearest, output, ordering)(us, vs)
    }


    def oNearestR
      (vs: BEDFILE,
       antimonotonic: ANTIMONOTONIC, 
       nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         nearest:  (Bed, List[Bed]) => List[Bed] = Genometric.MD(1),
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE): BEDFILE =
    { 
      BFOps.oNearestR(
        antimonotonic, 
        nonantimonotonic: _*)(nearest, output, ordering)(us, vs)
    }


    def coverR
      (minmax: Int => Bool, 
       aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool= true): BEDFILE =
    {
      BFOps.coverR(minmax, aggrs: _*)(stranded)(Seq(us))
    }


    def summitR
      (minmax: Int => Bool, 
       aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool= true): BEDFILE =
    {
      BFOps.summitR(minmax, aggrs: _*)(stranded)(Seq(us))
    }


    def histoR
      (minmax: Int => Bool,
       aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool = true): BEDFILE =
    {
      BFOps.histoR(minmax, aggrs: _*)(stranded)(Seq(us))
    }


    def flatR
      (minmax: Int => Bool, 
       aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool= true): BEDFILE =
    {
      BFOps.flatR(minmax, aggrs: _*)(stranded)(Seq(us))
    }


    def complementR(implicit stranded: Bool = true): BEDFILE =
      BFOps.complementR(stranded)(Seq(us))

  } // End class BFOps



  object BFOps {

    private def store(bf: BEDFILE): BEDFILE =
      if (STORE) bf.serialized else bf


    /** [[selectR(f)(us)]] in SQL-speak is: 
     *
     *  SELECT u.* FROM us as u WHERE f(u) 
     */

    def selectR(pred: Bed => Bool)(us: BEDFILE): BEDFILE = 
      store(us.filter(r => pred(r)).use(us))


    /** [[projectR(k1 -> f1, .., kn -> fn)(us)]] in SQL-speak is: 
     *
     *  SELECT k1 as f1(u), .., kn as fn(u) FROM us u
     */

    def projectR(proj: (String, Bed => _)*)(us: BEDFILE): BEDFILE = 
      store { 
        us.map { u => u.replaceMeta(proj.toMap map { case (k,f) => k->f(u) }) }
          .use(us)
    }
 

    /** [[extendR(k1 -> f1, .., kn -> fn)(us)]] in SQL-speak is:
     *
     *  SELECT u.*, k1 as f1(u), .., kn as fn(u) FROM us u
     */

    def extendR(proj: (String, Bed => _)*)(us: BEDFILE): BEDFILE =
      store { 
        us.map { u => u ++ (proj map { case (k, f) => k -> f(u) } : _*) }
          .use(us)
    }


    /** [[partitionbyR(k1 -> a1, .., kn -> an)(us)]] in SQL-speak is:
     *
     *  SELECT u.chrom, u.start, u.end,
     *         k1 as A1, .., kn  as An
     *  FROM us u
     *  GROUPBY u.chrom u.start, u.end
     *  WITH A1 = a1 applied to the group, ..,
     *       An = an applied to the group.
     *
     *  This function assumes the BED file [[us]] is already sorted
     *  on [[us.loc]].
     */

    def partitionbyR
      (aggrs: (String, AGGR[Bed,Any])*)
      (us: BEDFILE) : BEDFILE =
    {
      type META = Map[String,Any]
      val aggr  = OpG.combine(aggrs: _*)
      def output(l: Locus, m: META) = Bed(loc = l, strand = ".", misc = m)
      store { BedFile.transientBedFile(aggr.byPartition(us, output _)).use(us) }
    }


    /** [[partitionbyR(g, k1 -> a1, .., kn -> an)(us)]] in SQL is:
     *
     *  SELECT u.chrom, u.start, u.end, u.g, k1 as A1, .., kn  as An
     *  FROM us u
     *  GROUPBY u.chrom u.start, u.end, u.g
     *  WITH A1 = a1 applied to the group, ..,
     *       An = an applied to the group.
     *
     *  This function assumes the BED file [[us]] is already sorted
     *  on [[us.loc]].
     */

    def partitionbyR
      (grp: String, 
       aggrs: (String, AGGR[Bed,Any])*)
      (us: BEDFILE): BEDFILE = 
    {
      val aggr = OpG.combine(aggrs: _*)
      val e    = List[Bed]()
      def iter(b: Bed, bs: List[Bed]) = b::bs
      def done(bs: List[Bed]) = bs.groupBy(_(grp)).toSeq
      def output(kb: Locus, subgrps: Seq[(Any,List[Bed])]) =
        subgrps map {
            case (g, l) => Bed(loc    = kb, 
                               strand = ".", 
                               misc   = aggr(l) + ("group" -> g))
        }
      val cit = us.clusteredFold(e, iter _, done _, output _)
      store { BedFile.transientBedFile(cit.flatMap(bs => bs)).use(us) }
    }


    /** [[mapR(k1 -> a1, .., kn -> an)(ctr)(us, vs)]] in SQL is:
     *
     *  SELECT u.*, ctr as COUNT, k1 as A1, .., kn as An
     *  FROM us u, vs v
     *  WHERE u overlaps v 
     *  GROUPBY locus of u
     *  WITH A1 = a1 applied to the group, ..,
     *       An = an applied to the group,
     *       COUNT is size of the group
     *
     *  Notice that the "SQL" query above is a join of BED files [[us]]
     *  and [[vs]]. These flat files typically has tens or hundreds of 
     *  thousand of entries. As the join condition, overlap, is comparing
     *  two intervals, a naive implementation has quadratic complexity.
     *  Even with binning of regions into intervals, it is still quadratic.
     *
     *  Here, we use Synchrony iterator to perform synchronized iteration
     *  on the [[us]] and [[vs]] flat files directly.  The complexity
     *  becomes essentially linear.
     *
     *  This function assumes both [[us]] and [[vs]] are sorted on 
     *  their [[loc]] field.
     */ 

    def mapR
      (aggrs: (String, AGGR[Bed,Any])*)
      (implicit ctr: String = "count"): (BEDFILE, BEDFILE) => BEDFILE =
    {
      val tally = ctr -> OpG.COUNT[Bed]
      val aggr  = OpG.combine({ tally +: aggrs }: _*)
      (us: BEDFILE, vs: BEDFILE) => store {
         BedFile.transientBedFile {
           val tr = us join vs on overlap[Locus](1) where Bed.sameStrand
           tr.cbi.map { case (u, matches) => u ++ aggr(matches) }
                 .userev(us, vs)
/** The above [[val tr = ...; tr.cbi.map ...]] can also be implemented
 *  using the comprehension below:
 *  {{{
 *         for ((u, matches) <- us join vs on overlap[Locus](1) 
 *                                         where Bed.sameStrand)
 *         yield u ++ aggr(matches)
 * }}}
 */
        }
      }
    }

            

    /** [[differenceR(exact)(us, d1, .., dn)]] in SQL is:
     *
     *  SELECT u.*
     *  FROM us u
     *  WHERE exact && locus of u is not in any of d1, .., dn
     *     OR (!exact) && locus of u doe)s not overlap regions in d1, .., dn.
     *
     *  The "SQL" above actually is a multi-table join, as d1, .., dn
     *  are also BED files. For exact loci matching this can be quite 
     *  fast since it is easy to build index on loci. When exact = false,
     *  inexact matching (i.e. overlap) is required. Then a naive
     *  implementation has complexity n * |us x di|, i.e. quadratic.
     *
     *  Here we use Synchrony iterator to implement it. It synchronizes
     *  iterations on all these files at once. The complexity is linear,
     *  n * |di| + |us|.
     *
     *  This function requires all the BED files to be sorted on their
     *  [[loc]] field.
     */
    
    def differenceR
      (implicit exact: Bool = false): (BEDFILE, Seq[BEDFILE]) => BEDFILE =
    {
      val chkExact = if (exact) Bed.sameLocusNStrand else Bed.sameStrand

      (us: BEDFILE, diffs: Seq[BEDFILE]) => store {
        if (diffs.isEmpty) us 
        else BedFile.transientBedFile {
          val vs = diffs.head.mergedWith(diffs.tail: _*)
          val tr = us join vs on overlap[Locus](1) where chkExact
          tr.cbi.flatMap { case (u, Nil) => Some(u); case _ => None }
/** The above [[val tr = ...; tr.cbi.flatMap ...]] can also be implemented
 *  using the comprehension below:
 * {{{
 *          for (
 *          (u, matches) <- us join vs on overlap[Locus](1) where chkExact;
 *          if matches.isEmpty
 *        ) yield u
 * }}}
 */
        }.userev(us)
      }
    }
    


    /** [[joinR(ycs, ncs1, ..)(output, ordering)(us, vs)]]
     *
     *  SELECT output(u, v)
     *  FROM us u, vs v
     *  WHERE ycs(v, u) && 
     *        ncs1(v, u) && ncs2(v, u) && ..
     *        screen(u, v)        
     *  ORDERBY ordering
     *
     *  As can be seen, this is naively also a join on [[us]] and [[vs]].
     *  The genometric predicates [[ycs]] (which must be antimonotonic), 
     *  [[ncs1..]] (which are assumed nonantimonotonic), are constraints
     *  on the loci of regions u in [[us]] and v in [[vs]]; in particular,
     *  these are non-exact matching. So, naive implementation is quadratic.
     *
     *  Again, we use Synchrony iterator to perform synchronized iterations
     *  on [[us]] and [[vs]]. The complexity becomes essentially linear.
     */

    def joinR
      (antimonotonic: ANTIMONOTONIC, nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE)
    : (BEDFILE,BEDFILE) => BEDFILE =
    {
      val otherConds = Bed.requires({ Bed.sameStrand +: nonantimonotonic }: _*)
      
      (us: BEDFILE, vs: BEDFILE) => store {
        BedFile.transientBedFile {
          ordering {
            val tr = us join vs using antimonotonic where otherConds;
            tr.cbi.flatMap { case (u, ms) => ms.flatMap { output(u, _) } }
/** The above [[val tr = ...; tr.cbi.flatMap ...]] can also be implemented
 *  using the comprehension below:
 *  {{{
 *          for (
 *            (u, matches) <- us join vs using antimonotonic where otherConds;
 *            m <- matches;
 *            o <- output(u, m)
 *          ) yield o
 * }}}
 */
          }.userev(us, vs)
        }
      }
    }



    /** [[iNearestR(ycs, ncs1, ...)(nearest)(output, ordering)(us, vs)]]
     *  is similar to [[joinR]]. However, for each u in [[us]], among
     *  those v in [[vs]] satisfying ycs, ncs1, ..., it picks those
     *  which are closest to u and joins them with u.
     */
  
    def iNearestR
      (antimonotonic: ANTIMONOTONIC, nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         nearest:  (Bed, List[Bed]) => List[Bed] = Genometric.MD(1),
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE)
    : (BEDFILE,BEDFILE) => BEDFILE =
    {
      val otherConds = Bed.requires({ Bed.sameStrand +: nonantimonotonic }: _*)

      (us: BEDFILE, vs: BEDFILE) => store {
        BedFile.transientBedFile {
          ordering {
            val tr = us join vs using antimonotonic where otherConds;
            tr.cbi.flatMap { case (u,ms) => nearest(u,ms).flatMap(output(u,_)) }
/** The above [[val tr = ...; tr.cbi.flatMap ...]] can also be implemented
 *  using the comprehension below:
 *  {{{
 *          for (
 *            (u, matches) <- us join vs using antimonotonic where otherConds;
 *            m <- nearest(u, matches);
 *            o <- output(u, m)
 *          ) yield o
 * }}}
 */
          }
        }.userev(us, vs)
      }
    }



    /** [[oNearestR(ycs, ncs1, ...)(nearest)(output, ordering)(us, vs)]]
     *  is similar to [[joinR]]. However, for each u in [[us]], it first
     *  picks those v in [[vs]] which are nearest to u. Then it joins
     *  those also satisfying ycs, ncs1, ... with u.
     */
     
    def oNearestR
      (antimonotonic: ANTIMONOTONIC, nonantimonotonic: NONANTIMONOTONIC*)
      (implicit
         nearest:  (Bed, List[Bed]) => List[Bed] = Genometric.MD(1),
         output:   (Bed, Bed)  => Option[Bed] = BFOut.BOTH, 
         ordering: BEDITERATOR => BEDITERATOR = BFOrder.NONE)
    : (BEDFILE,BEDFILE) => BEDFILE =
    {
      val otherConds = Bed.requires(nonantimonotonic: _*)

      (us: BEDFILE, vs: BEDFILE) => store {
        BedFile.transientBedFile {
          ordering {
            val tr = us join vs using antimonotonic where Bed.sameStrand;
            tr.cbi.flatMap { case (u, ms) => nearest(u, ms)
                                            .filter(otherConds(_, u))
                                            .flatMap(output(u, _)) }
/** The above [[val tr = ...; tr.cbi.flatMap ...]] can also be implemented
 *  using the comprehension below:
 *  {{{
 *          for (
 *            (u,matches) <- us join vs using antimonotonic 
 *                                      where Bed.sameStrand;
 *            m <- nearest(u, matches).filter(otherConds(_, u));
 *            o <- output(u, m)
 *            ) yield o
 * }}}
 */
          }
        }.userev(us, vs)
      }
    }



    /** [[coverR(minmax, l1 -> a1, .., lk -> ak)(v1, .., vn)]]
     *
     *  Returns all (sub)regions appear m times in [[v1]], .., [[vn]]
     *  where [[minmax(m)]] holds. Apply [[a1]], ..., [ak]] to these
     *  regions.
     *
     *  This is implemented quite cleverly here. [[v1]], ..., [[vn]] are
     *  first merged in linear time, assuming they are sorted on loci
     *  already. Then the merged list of regions are mapped to a 
     *  duplicate-free version of itself using Synchrony iterator in
     *  linear time. With the map, it is easy to check if the start of
     *  a region is overlapped/covered by m other regions. If so, take 
     *  intersection of these m other regions and output the intersection.
     *  If not, just skip the region. Also, apply aggregate functions 
     *  [[a1]], .., [[ak]] to regions contributing to the intersection.
     */
 
    def mapCoverR
      (coverOp: BEDITERATOR => BEDITERATOR,
       aggrs: (String, AGGR[Bed,Any])*)
      (implicit
         overlap:  ANTIMONOTONIC = Genometric.OVERLAPSTART.apply _,
         screen:   (Bed, List[Bed]) => List[Bed] = BFCover.coverGenMin,
         stranded: Bool = true)
    : Seq[BEDFILE] => BEDFILE = 
    {
      /** Requires all BED files are sorted on loci; and
       *  [[overlap]] is antimonotonic.
       */

      (vs: Seq[BEDFILE]) => if (vs.length == 0) BedFile.emptyBedFile else { 

        val merged = if (vs.length == 1) vs.head.serialized
                     else vs.head.mergedWith(vs.tail: _*).serialized

        val flag = merged.protect

        def mm(it: CBI[Bed]) = { 
          val mit = if (stranded) it else BFOrder.NULLSTRAND(it)
          BedFile.transientBedFile(mit)
        }

        val refs   = {
          merged.protection(true)
          val st = mm(merged.map(_.chromStart).cbi)
          val en = mm(merged.map(_.chromEnd).cbi).ordered.protection(false)
          val se = st.mergedWith(en)
          val di = BFOrder.DISTINCT(se.cbi)
          BedFile.transientBedFile(di).serialized
        }
   
        val aggrRes =
        {
          val cit = 
            for((r, hs) <- refs join merged 
                                using overlap 
                                where {(v, u) => Bed.sameStrand(v, u) && 
                                                (v.loc endAfter u.loc) }; 
                 t <- screen(r, hs))
            yield t

          val coverRes = {
            val tr = if (stranded) BFOrder.withStrand(coverOp)(cit)
                     else coverOp(cit) 
            BedFile.transientBedFile(tr)
          }
           
          if (aggrs.length == 0) coverRes
          else mapR(aggrs: _*)("accIndex")(coverRes, merged)

        }.userev(merged.protection(flag), refs)

        val saved = aggrRes.serialized
        saved
      }
    }


    def coverR
      (minmax: Int => Bool, aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool = true)
    : Seq[BEDFILE] => BEDFILE =
    { 
      mapCoverR(BFOrder.COVER(minmax), aggrs: _*)(
                Genometric.OVERLAPSTART.apply _, 
                BFCover.coverGenMin, 
                stranded)
    }


    def summitR
      (minmax: Int => Bool, aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool = true)
    : Seq[BEDFILE] => BEDFILE =
    {
      mapCoverR(BFOrder.SUMMIT(minmax), aggrs: _*)(
                Genometric.OVERLAPSTART.apply _, 
                BFCover.coverGenMin, 
                stranded)
    }


    def histoR
      (minmax: Int => Bool, aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool = true)
    : Seq[BEDFILE] => BEDFILE =
    {
      mapCoverR(BFOrder.HISTO(minmax), aggrs: _*)(
                Genometric.OVERLAPSTART.apply _, 
                BFCover.coverGenMin, 
                stranded)
    }


    def flatR
      (minmax: Int => Bool, aggrs: (String, AGGR[Bed,Any])*)
      (implicit stranded: Bool = true)
    : Seq[BEDFILE] => BEDFILE =
    {
      mapCoverR(BFOrder.FLAT(minmax), aggrs: _*)(
                Genometric.OVERLAPSTART.apply _, 
                BFCover.coverGenMax, 
                stranded)
    }


    def complementR(implicit stranded: Bool = true): Seq[BEDFILE] => BEDFILE =
    {
      mapCoverR(BFOrder.COMPLEMENT)(
                Genometric.OVERLAPSTART.apply _,
                BFCover.coverGenMax, 
                stranded)
    }

  } // End object BFOps





  object BFOrder 
  {
    /** Provides the orderings to be used in joins.
     *
     *  [[NONE]]         - no ordering needed
     *  [[DISTINCT]]     - eliminate consecutive duplicates
     *  [[SORT]]         - ordering based on loci
     *  [[SORTDISTINCT]] - ordering based on loci, eliminate duplicate loci.
     *  [[PLUS]]         - eliminate negative strand
     *  [[MINUS]]        - eliminate positive strand
     *  [[NULLSTRAND]]   - set all strands to null "."
     *  [[COMPLEMENT]]   - regions between non-overlapping loci 
     *  [[HISTO(minmax)]]  - histogram of loci htIndex satisfying minmax
     *  [[SUMMIT(minmax)]] - peaks of loci htIndex saisfying minmax
     *  [[COVER(minmax)]] - merge adjacent loci if htIndex satisfies minmax
     *  [[FLAT(minmax)]] - merge overlapping loci if htINdex satisfies minmax
     */

    val NONE: BEDITERATOR => BEDITERATOR = it => it

    
    val DISTINCT: BEDITERATOR => BEDITERATOR = 
    { 
      _.step { it => 
        val h = it.next()
        while (h == it.head && it.hasNext) { it.next() }
        h
      }
    }
      
      
    def SORT: BEDITERATOR => BEDITERATOR = 
      BedFile.transientBedFile(_).ordered.cbi


    def SORTDISTINCT: BEDITERATOR => BEDITERATOR = it => DISTINCT(SORT(it))


    def PLUS: BEDITERATOR => BEDITERATOR = it => {
      def pos(b: Bed) = b.strand match {
        case "-" => List()
        case "+" => List(b)
        case _   => List(b.newStrand("+"))
      }
      DISTINCT { it.flatMap(pos _) }
    }


    def MINUS: BEDITERATOR => BEDITERATOR = it => { 
      def neg(b: Bed) = b.strand match {
        case "+" => List()
        case "-" => List(b)
        case _   => List(b.newStrand("-"))
      }
      DISTINCT { it.flatMap(neg _) }
    }


    def NULLSTRAND: BEDITERATOR => BEDITERATOR = it => 
      DISTINCT { it.map { b => if (b.strand == ".") b else b.newStrand(".") } }


    def withStrand
      (f: BEDITERATOR => BEDITERATOR): BEDITERATOR => BEDITERATOR =
    {
      it => val bf         = BedFile.transientBedFile(it).serialized
            val pluses     = BedFile.transientBedFile(f(PLUS(bf.cbi)))
            val minuses    = BedFile.transientBedFile(f(MINUS(bf.cbi)))
            val merged     = pluses.mergedWith(minuses)
            merged.cbi.userev(bf)
    } 


    def HISTO(minmax: Int => Bool): BEDITERATOR => BEDITERATOR = 
    {
      it => {
        val hasnx = () => {
          while (it.hasNext && !minmax(it.head.getMeta[Int]("htIndex"))) {
            it.next() 
          }
          it.hasNext
        }
        val nx = () => {
          var cur = it.next()
          val cnt = cur.getMeta[Int]("htIndex")
          def canMerge(x: Bed) = cur.overlap(0)(x) && 
                                 cnt == x.getMeta[Int]("htIndex")
          while (it.hasNext && canMerge(it.head)) {
            cur = cur.newEnd(it.next().end)
          }
          if (it.hasNext && cur.overlap(0)(it.head)) cur.newEnd(it.head.start)
          else cur
        }
        CBI(hasnx, nx).use(it)
      }   
    }
          

    def SUMMIT(minmax: Int => Bool): BEDITERATOR => BEDITERATOR =
    {
      it => {
        var up = true
        HISTO(minmax)(it).step { hit =>
          def isSummit(cur: Bed) = 
            if (cur.overlap(0)(hit.head)) {
              if (cur.getMeta[Int]("htIndex") > hit.head.getMeta[Int]("htIndex"))  {
                val tmp = up; up = false; tmp
              } 
              else { up = true; false }
            }
            else { val tmp = up; up = true; tmp }
          var cur = hit.next()
          while (hit.hasNext && !isSummit(cur)) { cur = hit.next() }
          cur
        }
      }
    }

          
    def COVER(minmax: Int => Bool): BEDITERATOR => BEDITERATOR =
    {
      HISTO(minmax)(_).step { it =>
        var acc = it.next()
        while (it.hasNext && acc.overlap(0)(it.head)) {
          acc = acc.newEnd(it.next().end)
        }
        acc
      }
    }

 
    def FLAT(minmax: Int => Bool): BEDITERATOR => BEDITERATOR =
    {
      HISTO(minmax)(_).step { it =>
        var acc = it.next()
        while (it.hasNext && acc.overlap(0)(it.head)) {
          val nxt = it.next()
          acc = acc.newLoc(acc.chrom,
                           acc.start min nxt.start, 
                           acc.end max nxt.end)
        }
        acc
      }
    }
        

    val COMPLEMENT: BEDITERATOR => BEDITERATOR = it => {
      var u: Bed  = null
      var v: Bed  = if (it.hasNext) { it.next() } else null
      def nx()    = Bed(Locus(u.chrom, u.end, v.start), "", 0, u.strand, Map())
      def hasnx() = it.hasNext && {
        var chk   = false
        do { u    = v
             v    = it.next()
             chk  = u.loc endBefore v.loc
        } while (!chk && it.hasNext)
        chk 
      }
      
      CBI(() => hasnx(), () => nx()).use(it)
    }

  } // End object BFOrder
  


  object BFOut 
  {
    /** Provides output formatting to be used with joins.
     *
     *  [[LEFT(u, v)]]  - output u
     *  [[RIGHT(u, v)]] - output v
     *  [[INT(u, v)]]   - output the intersection of u's and v's loci. 
     *  [[BOTH(u, v)]]  - output u after copying v's locus into u's metadata
     *  [[CAT(u, v)]]   - output the concatenation of u's and v's loci,
     *                   and both u's and v's metadata.   
     */

    val LEFT  = (x: Bed, y: Bed) => Some(x)


    val RIGHT = (x: Bed, y: Bed) => Some(y)


    // Scala [[implicit f: String => String]] somehow always default
    // to identity function. So, cannot writing some thing like this:
    //
    // def intersect(implicit f: String => String = { "r." + _ } ) =
    // {
    //   (x: Bed, y: Bed) => (x.loc intersect y.loc) match {
    //     case None    => None
    //     case Some(l) => Some {
    //       Bed(l, x.name, x.score, x.strand, x.misc ++ y.renamedMeta(f))
    //     }
    //   }
    // }

    def intersectf(f: String => String = { "r." + _ } ) =
    {
      (x: Bed, y: Bed) => (x.loc intersect y.loc) match {
        case None    => None
        case Some(l) => Some {
          Bed(l, x.name, x.score, x.strand, x.misc ++ y.renamedMeta(f))
        }
      }
    }

    val INT = intersectf()


    def bothf(f: String => String = { "r." + _ }) =
    {
      (x: Bed, y: Bed) => Some {
        x ++ { y.renamedMeta(f) ++ 
               Map(f("chrom")->y.chrom, f("start")->y.start, f("end")->y.end) }
      }
    }

    val BOTH = bothf()


    def catf(f: String => String = { "r." + _ }) =
    {
      (x: Bed, y: Bed) => Some {
        val loc = Locus(x.chrom, x.start min y.start, x.end max y.end)
        Bed(loc, x.name, x.score, x.strand, x.misc ++ y.renamedMeta(f))
      }
    }

    val CAT = catf()

  }  // End object BFOut




  object BFCover
  {
    /** Functions for implementiting [[coverR]] and related functions.
     */

    val coverGenMin  = (r: Bed, hs: List[Bed]) =>
      if (hs.isEmpty) { List() }
      else {
        val loc = Locus(r.chrom, r.start, hs.map(_.end).min)
        List(Bed(loc, "", 0, r.strand, Map("htIndex" -> hs.length)))
      }


    val coverGenMax = (r: Bed, hs: List[Bed]) =>
      if (hs.isEmpty) { List() }
      else {
        val loc = Locus(r.chrom, hs.map(_.start).min, hs.map(_.end).max)
        List(Bed(loc, "", 0, r.strand, Map("htIndex" -> hs.length)))
      }
    

    /** Constraints to be used in [[coverR]] and related functions.
     */

    def ATLEAST(min: Int) = (n: Int) => n >= min 


    def ATMOST(max: Int) = (n: Int) => n <= max


    def BETWEEN(min: Int, max: Int) = (n: Int) => min <= n && n <= max 


    def EXACTLY(m: Int) = (n: Int)  => n == m


  }  // End object BFCover



  object Genometric
  {

    /** Genometric predicates for use with [[iNearestR]] 
     *  and [[oNearestR]] only.
     *
     *  [[UPSTREAM(n)(x,hs)]]   - n nearest entries in hs up-stream of x
     *  [[DOWNSTREAM(n)(x,hs)]] - n nearest entries in hs down-stream of x
     *  [[MD(n)(x,hs)]]         - n nearest entries in hs up-/down-stream
     *                            of x
     */

    def UPSTREAM(n: Int) = (x: Bed, hs: List[Bed]) =>  
      hs.filter(_ endBefore x).takeRight(n)
    
  
    def DOWNSTREAM(n: Int) = (x: Bed, hs: List[Bed]) => 
      hs.filter(_ startAfter x).take(n)
   

    def MD(n: Int) = (x: Bed, hs: List[Bed]) => {
      hs.filter(h => (h endBefore x) || (h startAfter x))
        .sortWith((u, v) => u.distFrom(x) < v.distFrom(x))
        .take(n)
    }



    /** List of general "genometric" predicates, for convenience 
     */

    /** Antimonotonic predicates
     */

    val BEFORE          = Locus.isBefore[Locus]
    val SAMECHROM       = Locus.sameChrom[Locus]
    val SAMELOCUS       = Locus.sameLocus[Locus]
    val SAMESTART       = Locus.sameStart[Locus]
    val STARTBEFORE     = Locus.startBefore[Locus]
    val OVERLAPSTART    = Locus.overlapStart[Locus]
    
    def OVERLAP(n: Int) = Predicates.overlap[Locus](n)
    def NEAR(n: Int)    = Predicates.near[Locus](n)

    val EQ              = Predicates.eq[Locus]
    val LT              = Predicates.lt[Locus]
    val LTEQ            = Predicates.lteq[Locus]

    def DL(n: Int)      = Predicates.dl[Locus](n)
    def DLEQ(n: Int)    = Predicates.dleq[Locus](n)

    def SZPERCENT(n: Double) = Predicates.szpercent[Locus](n)


    /** Non-antimonotonic predicates
     */

    val STARTAFTER      = Locus.startAfter[Locus]
    val ENDBEFORE       = Locus.endBefore[Locus]
    val ENDAFTER        = Locus.endAfter[Locus]
    val SAMEEND         = Locus.sameEnd[Locus]
    val OVERLAPEnd      = Locus.overlapEnd[Locus]

    def FAR(n: Int)     = Predicates.far[Locus](n)
    val INSIDE          = Predicates.inside[Locus]
    val ENCLOSE         = Predicates.enclose[Locus]
    val TOUCH           = Predicates.touch[Locus]
    val OUTSIDE         = Predicates.outside[Locus] 

    val GT              = Predicates.gt[Locus]
    val GTEQ            = Predicates.gteq[Locus]

    def DG(n: Int)      = Predicates.dg[Locus](n)
    def DGEQ(n: Int)    = Predicates.dgeq[Locus](n)


    /** Implicit conversions between genometric predicates and
     *  their function form [[(Locus,Locus)=>Bool]] and
     *  [[(Bed,Bed)=>Bool]].
     */

    implicit def ToANTIMONO(pred: AntimonotonicPred[Locus]): ANTIMONOTONIC =
      pred.apply _

    implicit def ToNONANTI1(pred: GenPred[Bed,Bed]): NONANTIMONOTONIC =
      pred.apply _

    implicit def ToNONANTI2(pred: AntimonotonicPred[Locus]): NONANTIMONOTONIC =
      (v: Bed, u: Bed) => pred(v.loc, u.loc)

  }  // End object Genometric



  object BFAggr
  {
    val COUNT: AGGR[Bed,Int] = OpG.COUNT[Bed]

    def SUMInt(f: Bed => Int):  AGGR[Bed,Int] = OpG.SUM[Bed,Int](f)

    def SUM(f: Bed => Double): AGGR[Bed,Double] = OpG.SUM[Bed,Double](f)

    def AVERAGE(f: Bed => Double): AGGR[Bed,Double] = OpG.AVERAGE[Bed](f)

    def SMALLESTInt(f: Bed => Int): AGGR[Bed,Int] = OpG.SMALLEST[Bed,Int](f)

    def SMALLEST(f: Bed => Double): AGGR[Bed,Double] =
      OpG.SMALLEST[Bed,Double](f)

    def BIGGESTInt(f: Bed => Int): AGGR[Bed,Int] = OpG.BIGGEST[Bed,Int](f)

    def BIGGEST(f: Bed => Double): AGGR[Bed,Double] = OpG.BIGGEST[Bed,Double](f)

    def MINIMIZE(f: Bed => Double): AGGR[Bed,(Double,List[Bed])] =
      OpG.MINIMIZE[Bed,Double](f)

    def MAXIMIZE(f: Bed => Double): AGGR[Bed,(Double,List[Bed])] =
      OpG.MAXIMIZE[Bed,Double](f)

  }  // End BFAggr


  object implicits 
  {
    implicit def metaR[A](k: String) = (b: Bed) => b.getMeta[A](k)


    // Simple tricks to endow Bed with ENCODE attributes

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

  }


} // End object BEDFileOps






/** Examples *********************************************
 *
{{{

import gmql.BEDFileOps._
import BFOps._
import Genometric._
import gmql.BEDFileOps.implicits.metaR
import gmql.BEDModel.BEDFile._
import gmql.BEDModel.BEDFile.implicits._
import gmql.BEDModel.BEDEntry._
import dbmodel.DBModel.OpG._
import dbmodel.DBFile.implicits._


implicit class PRINTER(bs: BEDFILE) {
  def print(n:Int) = bs.cbi.done {
    // Use .done to execute this, to ensure file is autoclosed.
    _.take(n).foreach(b => { println(s"** $b **\n") })
  }
}


// Read an ENCODE file

val bf1374  = BedFile.encode("test/hepg2_np_ctcf/files/1374_narrowpeaks.bed")


// # of BED entries for chr22. Print 1st 3.

bf1374.selectR(_.chrom == "chr22").length


bf1374.selectR(_.chrom == "chr22").print(3)


// Projection.

bf1374.projectR("name" -> "name", "signal" -> "signalval").print(3)


// Extension.

bf1374.extendR("start-end" -> { r => s"${r.start} - ${r.end}" }).print(3)


// Partition based on locus.
// Serialize the partition results and use it many times.

val bf1374P = bf1374.partitionbyR(
                    "count" -> COUNT[Bed],
                    "max"   -> BIGGEST[Bed,Double]("signalval")).serialized

bf1374P.print(3)


// Biggest "max" field

biggest((b: Bed) => b.getDbl("max"))(bf1374P)


// Which BED entries have the biggest "max" field

maximize((b: Bed) => b.getMeta[Double]("max"))(bf1374P)


// Selection

bf1374P.selectR(_.getMeta[Double]("max") > 30).print(3)


bf1374P.close()  // finished using this file. delete it.



// Partition based on locus + strand

bf1374.partitionbyR("strand",
                    "peak" -> SMALLEST[Bed,Int]("peak"),
                    "max" -> BIGGEST[Bed,Double]("signalval")).print(3)


// Map

bf1374.mapR(bf1374).print(3)


{ bf1374.mapR(bf1374, 
            "max"  -> BIGGEST[Bed,Double]("signalval"),
            "peak" -> SMALLEST[Bed,Int]("peak"))
        .print(3)
}


{ val mm = bf1374.mapR(bf1374 mergedWith bf1374)
  mm.print(3)
  mm.close()
}


// Difference

bf1374.differenceR(bf1374.selectR(_.chrom == "chr1")).print(3)


// Joins.

{ val mm = bf1374.joinR(bf1374, OVERLAP(1))(ordering = BFOrder.DISTINCT)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.iNearestR(bf1374, NEAR(1000))(nearest = MD(2))
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.oNearestR(bf1374, NEAR(1000))(nearest = MD(2))
  mm.print(3)
  mm.close()
}


// Cover functions.

{ val mm = bf1374.coverR(BFCover.ATLEAST(1))(stranded = false)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.coverR(
             BFCover.ATLEAST(1), 
             "ctr" -> COUNT[Bed])(stranded = false)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.summitR(BFCover.ATLEAST(1))(stranded = false)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.histoR(BFCover.ATLEAST(1))(stranded = true)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.flatR(BFCover.ATLEAST(1))(stranded = true)
  mm.print(3)
  mm.close()
}


{ val mm = bf1374.complementR(stranded = false)
  mm.print(5)
  mm.close()
}


}}}
 *
 */


