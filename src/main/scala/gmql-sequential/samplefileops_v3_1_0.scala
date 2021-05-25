
package synchrony.gmql


/** Synchrony GMQL
 *
 *  This package provides operations on samples and tracks. It
 *  is an emulation of GMQL, ignoring some syntax idiosyncrasies. 
 *  The operations on samples and tracks (genomic regions) are
 *  made orthogonal, resulting in a more compact implementation. 
 *
 *  Conceptually, the data manipulated can be thought of as a
 *  simple 1-level nested relation, 
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
 *                       a row in a BED file;
 *        EFile can be thought of as a "file-based vector",
 *        which transparently materializes the parts of the
 *        vector of samples and BED rows into memory when
 *        they are needed in a computation.
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
 *  It this emulation, GMQL operations are "orthogonalized"
 *  into queries on samples (selectS, projectS, groupbyS,
 *  joinS, mapS, differenceS, and onRegion) and tracks (selectR, 
 *  projectS, partitionbyR, joinR, mapR, and differenceR.)
 *
 *  The first group is presented in this package; 
 *  each operation takes one or more EFile[Sample] as input
 *  and produces an EFile[Sample] as output. Most of them
 *  have an "onRegion" parameter, which is for a user to
 *  supply a function in the second group (or any function
 *  that takes EFile[Bed] and produces EFile[Bed].) This way,
 *  we deconvolute any GMQL query into its two components,
 *  viz. a relational query on the (outer) EFile[Sample] relation
 *  and a relational query on the (inner) EFile[Bed] relation
 *  nested inside each sample. This orthogonality greater 
 *  simplifies understanding of GMQL queries and the emulation.
 *
 *  Also included in the first group is the onRegion function,
 *  which takes a query in the second group, and applies it
 *  to the track/BED file of each sample. It corresponds to
 *  GMQL queries that touch the tracks of samples but don't do
 *  anything to samples themselves.
 *
 *  The second group (the one that operates on tracks) is
 *  provided in another package, BedFileOps. This facilitates
 *  the BedFileOps to be used separately (i.e. BED files), 
 *  without bundling together with samples (BED files can be
 *  used in other contexts.)
 *
 * Wong Limsoon
 * 21 April 2021
 */



object SampleFileOps {


  import synchrony.gmql.Samples
  import synchrony.gmql.SampleFiles
  import synchrony.programming.Sri.withAcc
  import synchrony.genomeannot.BedFileOps._
  
  //
  // Put a copy of packages and data types which 
  // are expected to be used with SampleFile here.
  //

  val Sample     = Samples.Sample
  val SampleFile = SampleFiles.SampleFile
  val BedFileOps = synchrony.genomeannot.BedFileOps 
  val BedFile    = BedFileOps.BedFile
  val Bed        = BedFileOps.Bed
  val OpG        = BedFileOps.OpG
  val BFOut      = BedFileOps.BFOut
  val BFCover    = BedFileOps.BFCover
  val BFOrder    = BedFileOps.BFOrder

  type Sample          = Samples.Sample
  type SampleFile      = SampleFiles.SampleFile      // EFile[Sample]
  type SampleEIterator = SampleFiles.SampleEIterator // EIterator[Sample]
  type Bed             = BedFileOps.Bed
  type BedFile         = BedFileOps.BedFile          // EFile[Bed]
  type BedEIterator    = BedFileOps.BedEIterator     // EIterator[Bed]
  type Aggr[C, F]      = BedFileOps.Aggr[C, F]



/** Endow EFile[Sample] with GMQL operations.
 */

  implicit class SFOps(samples: SampleFile) {


/** tracksSorted gets all the tracks in the samples sorted by loci.
 *  Synchrony iterators assume all tracks are sorted.
 */

    def tracksSorted: SampleFile = onRegion(_.sorted)

    def tracksSortedIfNeeded: SampleFile = {
      val tmp = samples.serialized
      tmp.tracksAreSorted match {
        case true  => tmp
        case false => tmp.tracksSorted
      }
    }


/** tracksAreSorted checks whether all the tracks are sorted by loci.
 */

    def tracksAreSorted: Boolean = samples.eiterator.forall(_.bedFile.isSorted)




/** tracksSaved saves the tracks in the samples, generating
 *  BED files for them.
 */
 
    def tracksSaved(nameOf: Sample => String): SampleFile = 
      SampleFile.transientSampleFile(
        for (s <- samples.eiterator)
        yield s.trackSavedAs(nameOf(s))
      )



/** materialized produces the SampleFile and serializes it to disk.
 *
 *  This GMQL emulation is lazy. That is, query results are
 *  not produced all at once. They are produced one at a
 *  time, and are accessible once only, in a stream-like manner. 
 *  "Materialization" generates *  the results all at once and
 *  serializes them to a physical SampleFile on disk or in memory.
 */

    def materialized: SampleFile = samples.serialized

    def toVector: Vector[Sample] = samples.eiterator.toVector



/** onRegion(f) applies a query f on every track in samples.
 */

    def onRegion(f: BedFile => BedFile): SampleFile = 
      SFOps.onRegion(samples)(f)


/** selectS(f)
 *
 *  SELECT s.* FROM samples s WHERE f(s)
 */

    def selectS(
      onSample: Sample => Boolean = (s: Sample) => true
    ): SampleFile = SFOps.selectS(samples)(onSample)


/** projectS(l1 -> f1, .., ln -> fn)
 *
 *  SELECT l1 as f1(s), .., ln as fn(s) FROM samples s
 */

    def projectS(
      onSample: (String, Sample => Any)*
    ): SampleFile = SFOps.projectS(samples)(onSample: _*)


/** extendS(l1 -> f1, .., ln -> fn)
 *
 *  SELECT s.*, l1 as f1(s), .., ln as fn(s) FROM samples s
 */

    def extendS(
      onSample: (String, Sample => Any)*
    ): SampleFile = SFOps.extendS(samples)(onSample: _*)


/** groupbyS(g, l1 -> a1, .., ln -> an)
 *
 *  SELECT group as s.g, l1 as A1, .., ln as An
 *  FROM samples s
 *  GROUPBY s.g
 *  WITH A1 = results of aggregate function a1 on a group, ..,
 *       An = results of aggregate function an on a group
 */

    def groupbyS(
      grp: String, 
      aggr: (String, Aggr[Sample, Any])*
    ): SampleFile = SFOps.groupbyS(samples)(grp, aggr: _*)


/** differenceS(that)(joinby, exact)
 *
 *  SELECT s.*, 
 *         differenceR(exact)(s.BedFile,
 *                            SELECT t.BedFile FROM that t WHERE joinby(s, t))
 *  FROM samples s
 *
 *  differenceR(exact)(us, d1, .., dn) is this pseudo SQL query on
 *  us:BedFile, d1:BedFile, ...dn:BedFile,
 *
 *  SELECT u.*
 *  FROM us u
 *  WHERE exact && locus of u is not in any of d1, .., dn
 *     OR (!exact) && locus of u does not overlap any region in d1, .., dn.
 */

    def differenceS
      (that: SampleFile)
      (implicit
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
         exact: Boolean = false)
    : SampleFile = SFOps.differenceS(samples, that)(joinby, exact)


/** joinS(that)(f, g, joinby)
 *
 *  SELECT g(s, t).bedFileUpdated(f(s.BedFile, t.BedFile))
 *  FROM samples s, that t
 *  WHERE joinby(s,t)
 */

    def joinS
      (that: SampleFile)
      (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile, vs:BedFile) => us,
       onSample: (Sample, Sample) => Sample = SFOut.overwrite,
       joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
    : SampleFile = SFOps.joinS(samples, that)(onRegion, onSample, joinby)


/** mapS(that)(f, g, joinby)
 *
 *  SELECT g(s, t).bedFileUpdated(f(s.BedFile, t.BedFile))
 *  FROM samples s, that t
 *  WHERE joinby(s,t)
 *
 *  It is actually same as joinS. However, f is given a default
 *  function that corresponds to mapR. I.e. f is by default the
 *  following query on s.bedFile:
 *
 *  SELECT u.* count as COUNT
 *  FROM s u, t v
 *  WHERE u overlaps v
 *  GROUPBY locus of u
 *  WITH  COUNT = size of a group
 */

    def mapS
      (that: SampleFile)
      (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
       onSample: (Sample, Sample) => Sample = SFOut.left,
       joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
    : SampleFile = SFOps.mapS(samples, that)(onRegion, onSample, joinby)



/** coverS(g, onRegion) groups samples by metadata attribute g,
 *  then applies the function onRegion on the tracks of each group.
 *  Usually, onRegion is the coverR method in BFOps.
 *
 *  GMQL: COVER(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
 *  In Synchrony, this is expressed as:
 *  {
       import BFOps.coverR, BFCover.between
       db.coverS(
         onRegion=coverR(between(2,3), "minPVal"->smallest("pval")),
         groupby="cell")
 *  }
 *
 *  GMQL: FLAT(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
 *  In Synchrony, this is expressed as:
 *  {
       import BFOps.flatR, BFCover.between 
       db.coverS(
         onRegion=flatR(between(2,3), "minPVal"->smallest("pval")),
         groupby="cell")
 *  }
 *
 *  GMQL: SUMMIT(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
 *  In Synchrony, this is expressed as:
 *  {
       import BFOps.summitR, BFCover.between 
       db.coverS(
         onRegion=summitR(between(2,3), "minPVal"->smallest("pval")),
         groupby="cell")
 *  }
 *
 *  GMQL: HISTOGRAM(2,3; groupby: cell; aggregate: minPVal as MIN(pval)) db
 *  In Synchrony, this is expressed as:
 *  {
       import BFOps.histoR, BFCover.between 
       db.coverS(
         onRegion=histoR(between(2,3), "minPVal"->smallest("pval")),
         groupby="cell")
 *  }
 *
 */

    def coverS(onRegion: Seq[BedFile] => BedFile, groupby: String*)
    : SampleFile = SFOps.coverS(samples, groupby: _*)(onRegion)

  }



/** Here is the implementation of the operations provided above.
 */

  object SFOps {


    def onRegion(samples: SampleFile)(f: BedFile => BedFile): SampleFile = 
      SampleFile.transientSampleFile {
        samples.eiterator.map { s =>
          s.bedFileUpdated(f(s.bedFile))
        }
      }



    def selectS
      (samples: SampleFile)
      (onSample: Sample => Boolean = (s: Sample) => true)
    : SampleFile = SampleFile.transientSampleFile(
        entries = for (s <- samples.eiterator; if onSample(s)) yield s,
        settings = samples.efile.settings
      )
    


    def projectS
      (samples: SampleFile)
      (onSample: (String, Sample => Any)*)
    : SampleFile = SampleFile.transientSampleFile(
        entries = for (
                    s <- samples.eiterator;
                    ps = onSample map { kf => val (k, f) = kf; (k, f(s)) };
                    ns = s.mErased ++ (ps: _*)
                  ) yield ns,
        settings = samples.efile.settings
      )


    def extendS
      (samples: SampleFile)
      (onSample: (String, Sample => Any)*)
    : SampleFile = SampleFile.transientSampleFile (
        entries = for (
                    s <- samples.eiterator;
                    ps = onSample map { kf => val (k, f) = kf; (k, f(s)) };
                    ns = s ++ (ps: _*)
                  ) yield ns,
        settings = samples.efile.settings
      )


    private def groupbySAux
      (samples: SampleFile)
      (grp: String)
    : SampleFile = {
      import synchrony.iterators.AggrCollections.implicits._
      val l = (s: Sample) => s[Any](grp)
      val p = for (
                (k, acc) <- samples.groupby(l)(OpG.keep);
                s <- acc;
                ns = s ++ ("group" -> k)
              ) yield ns
      SampleFile.inMemorySampleFile(
          entries = p.toVector,
          settings = samples.efile.settings)
    }


    def groupbyS
      (samples: SampleFile)
      (grp: String, 
       aggr: (String, Aggr[Sample, Any])*)
    : SampleFile = aggr.length match {

      case 0 => groupbySAux(samples)(grp)

      case _ => 
        import synchrony.iterators.AggrCollections.implicits._
        val f = aggr.combined
        val l = (s: Sample) => s[Any](grp)
        val p = for (
                  (k, (c, acc)) <- samples.groupby(l)(withAcc(f));
                  m = ("group" -> k) +: (c.toSeq);
                  s <- acc;
                  ns = s ++ (m: _*)
                ) yield ns
        SampleFile.inMemorySampleFile(
          entries = p.toVector,
          settings = samples.efile.settings)
    }


    def differenceS
      (samples: SampleFile, that: SampleFile)
      (implicit
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
         exact: Boolean = false)
    : SampleFile = SampleFile.transientSampleFile {
      val theseSamples = samples.serialized
      val otherSamples = that.serialized
      for (
        s <- theseSamples.eiterator;
        ts = otherSamples.filtered(t => joinby(s, t));
        if !ts.isEmpty;
        bs = ts.eiterator.map(t => t.bedFile).toSeq;
        nb  = s.bedFile.differenceR(bs: _*)(exact)
      ) yield s.bedFileUpdated(nb)
    }


    def joinS
      (samples: SampleFile, that: SampleFile)
      (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile, vs:BedFile) => us,
       onSample: (Sample, Sample) => Sample = SFOut.overwrite,
       joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
    : SampleFile = SampleFile.transientSampleFile {
      val theseSamples = samples.serialized
      val otherSamples = that.serialized
      for (
        s <- theseSamples.eiterator;
        t <- otherSamples.eiterator;
        if joinby(s, t);
        ns = onSample(s, t);
        nb = onRegion(s.bedFile, t.bedFile)
      ) yield ns.bedFileUpdated(nb)
    }


    def mapS
      (samples: SampleFile, that: SampleFile)
      (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
       onSample: (Sample, Sample) => Sample = SFOut.left,
       joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
    : SampleFile = joinS(samples, that)(onRegion, onSample, joinby)



    def coverS
      (samples: SampleFile,
       groupby: String*)
      (onRegion: Seq[BedFile] => BedFile = BFOps.coverR(BFCover.atleast(2)))
    : SampleFile = groupby.length match {
      
      case 0 => SampleFile.inMemorySampleFile {
        val m = Map[String, Any]()
        val b = onRegion(samples.eiterator.map(_.bedFile).toSeq)
        Vector(Sample(m, b))
      }
        
      case _ => SampleFile.transientSampleFile {
        import synchrony.iterators.AggrCollections.implicits._
        val l = (s: Sample) => Map(groupby.map(g => g -> s[Any](g)): _*)
        val p = for (
                  (m, acc) <- samples.groupby(l)(OpG.keep);
                  b = onRegion(acc.map(_.bedFile).toSeq))
                yield Sample(m, b)
        SampleFile.inMemorySampleFile(p.toVector)
      }
    }


  } // End object SFOps



/** SFSemiJoin provides a semi-join.
 */

  implicit class SFSemiJoin(sample: Sample) {
    def semijoin(in: String*)(ex: String*)(db: SampleFile) =
      db.eiterator.exists { s =>
        in.forall { l => s(l) == sample(l) } &&
        ex.forall { l => s(l) != sample(l) }
      }

    def apply(in: String*)(ex: String*)(db: SampleFile) =
      semijoin(in: _*)(ex: _*)(db)
  }


/** SFOut provides the common ways for producing results
 *  from joining and grouping samples.
 */

  object SFOut {
  
    // For use with joins

    val left = (s: Sample, t: Sample) => s

    val right = (s: Sample, t: Sample) => t

    val overwrite = (s: Sample, t: Sample) => 
      s mOverwritten t.mRenamed((p: String) => s"r.${p}").meta

    def overwrite(f: String => String = (p => "r." ++ p)) =
      (s: Sample, t: Sample) => s mOverwritten t.mRenamed(f).meta
  }
      


/** Functions to facilitate more readable syntax when
 *  using the above to express queries.
 */

  import scala.language.implicitConversions

  implicit  def metaS[A](k: String) = (s: Sample) => s[A](k)
  // Get the metadata k of a sample

  implicit def joinbyS(k: String) = (s: Sample, t: Sample) =>
     s[Any](k) == t[Any](k)

  implicit  def aggrS[A](a: Aggr[Bed, A]) = (s: Sample) => a(s.track)
  // Compute an aggregate function on a sample's track

  implicit def SampleFile2EIter(samples: SampleFile): SampleEIterator =
      samples.eiterator

} // SampleFileOps




