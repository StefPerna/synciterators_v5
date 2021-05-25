
package synchrony.pargmql

/** The SampleFileOps package is "lazy". I.e. query results are
 *  not materialized/sorted by default. This allows large result
 *  sets to be generated, streamed, and processed without being
 *  stored.  However, this "laziness" may not be natural to
 *  some programmers.
 *
 *  So, here is a package which is just a wrapper around
 *  SampleFileOps to always materialized query results.
 *
 *  If you always want to materialize results, do:
 *    import synchrony.gmql.SampleFileOpsImplicit.AlwaysMaterialize._
 *
 *  If you always want to sort results, do:
 *    import synchrony.gmql.SampleFileOpsImplicit.AlwaysSortIfNeeded._
 *
 * Wong Limsoon
 * 21 May 2021
 */


object SampleFileParOpsImplicit {

/** Use parallel SampleFileParOps as SampleFileOps
 *  Use sequential BedFileOps as BedFileOps
 */

  import synchrony.pargmql.{ SampleFileParOps => SampleFileOps }
  import synchrony.genomeannot.BedFileOps
  import synchrony.genomeannot.BedFileOps.implicits._
 
  type SampleFile = SampleFileOps.SampleFile

  class SFOpsWithImplicitAction[A](mm: (=> SampleFile) => A) {

    val SampleFile       = SampleFileOps.SampleFile
    val Sample           = SampleFileOps.Sample
    val BedFile          = BedFileOps.BedFile
    val Bed              = BedFileOps.Bed
    val OpG              = BedFileOps.OpG
    val BFOps            = BedFileOps.BFOps
    val BFOut            = BedFileOps.BFOut
    val BFCover          = BedFileOps.BFCover
    val BFOrder          = BedFileOps.BFOrder
    val SFOpsExplicit    = SampleFileOps.SFOps
    val SFOut            = SampleFileOps.SFOut

    type Sample          = SampleFileOps.Sample
    type SampleFile      = SampleFileOps.SampleFile 
    type SampleEIterator = SampleFileOps.SampleEIterator
    type Bed             = BedFileOps.Bed
    type BedFile         = BedFileOps.BedFile
    type BedEIterator    = BedFileOps.BedEIterator
    type Aggr[C, F]      = BedFileOps.Aggr[C, F]
    type SFOpsExplicit   = SampleFileOps.SFOps

    implicit class SFOps(samples: SampleFile) {

      private val sf = new SFOpsExplicit(samples)

      def tracksSorted: A = mm { 
        sf.tracksSorted
      }

      def tracksSortedIfNeeded: A =  mm { 
        sf.tracksSortedIfNeeded
      }

      def tracksAreSorted: Boolean = sf.tracksAreSorted
 
      def tracksSaved(nameOf: Sample => String): A = mm {
        sf.tracksSaved(nameOf)
      }

      def materialized: A = mm {
        samples.serialized
      }

      def toVector: Vector[Sample] = samples.toVector

      def onRegion(f: BedFile => BedFile): A = mm {
        sf.onRegion(f) 
      }

      def selectS(onSample: Sample => Boolean = (s: Sample) => true) : A = mm { 
        sf.selectS(onSample)
      }

      def projectS(onSample: (String, Sample => Any)*): A = mm { 
        sf.projectS(onSample: _*)
      }

      def extendS(onSample: (String, Sample => Any)*): A = mm { 
        sf.extendS(onSample: _*)
      }

      def groupbyS(
        grp: String, 
        aggr: (String, Aggr[Sample, Any])*
      ): A = mm {
        sf.groupbyS(grp, aggr: _*)
      }

      def differenceS
        (that: SampleFile)
        (implicit
           joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
           exact: Boolean = false)
      : A = mm {
        sf.differenceS(that)(joinby, exact)
      }

      def joinS
        (that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile,vs:BedFile)=>us,
         onSample: (Sample, Sample) => Sample = SFOut.overwrite,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : A = mm {
        sf.joinS(that)(onRegion, onSample, joinby)
      }

      def mapS
        (that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
         onSample: (Sample, Sample) => Sample = SFOut.left,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : A = mm {
        sf.mapS(that)(onRegion, onSample, joinby)
      }

      def coverS(
        onRegion: Seq[BedFile] => BedFile,
        groupby: String*)
      : A = mm {
        sf.coverS(onRegion, groupby: _*)
      }

    }


    object SFOps {

      def onRegion(samples: SampleFile)(f: BedFile=>BedFile): A = mm { 
        SFOpsExplicit.onRegion(samples)(f)
      }

      def selectS
        (samples: SampleFile)
        (onSample: Sample => Boolean = (s: Sample) => true)
      : A = mm {
        SFOpsExplicit.selectS(samples)(onSample)
      }

      def projectS
        (samples: SampleFile)
        (onSample: (String, Sample => Any)*)
      : A = mm {
        SFOpsExplicit.projectS(samples)(onSample: _*)
      }

      def extendS
        (samples: SampleFile)
        (onSample: (String, Sample => Any)*)
      : A = mm {
        SFOpsExplicit.extendS(samples)(onSample: _*)
      }

      def groupbyS
        (samples: SampleFile)
        (grp: String, 
         aggr: (String, Aggr[Sample, Any])*)
      : A = mm {
        SFOpsExplicit.groupbyS(samples)(grp, aggr: _*)
      }
    
      def differenceS
        (samples: SampleFile, that: SampleFile)
        (implicit
           joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
           exact: Boolean = false)
      : A = mm {
        SFOpsExplicit.differenceS(samples, that)(joinby, exact)
      }
    
      def joinS
        (samples: SampleFile, that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile, vs:BedFile) => us,
         onSample: (Sample, Sample) => Sample = SFOut.overwrite,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : A = mm {
        SFOpsExplicit.joinS(samples, that)(onRegion, onSample, joinby)
      }
      
      def mapS
        (samples: SampleFile, that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
         onSample: (Sample, Sample) => Sample = SFOut.left,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : A = mm {
        SFOpsExplicit.mapS(samples, that)(onRegion, onSample, joinby)
      }

      def coverS
        (samples: SampleFile,
         groupby: String*)
        (onRegion: Seq[BedFile] => BedFile)
      : A = mm {
        SFOpsExplicit.coverS(samples, groupby: _*)(onRegion)
      }

    } // End SFOps


    import scala.language.implicitConversions

    implicit  def metaS[A](k: String) = (s: Sample) => s[A](k)
    // Get the metadata k of a sample

    implicit def joinbyS(k: String) = (s: Sample, t: Sample) =>
       s[Any](k) == t[Any](k)

    implicit  def aggrS[A](a: Aggr[Bed, A]) = (s: Sample) => a(s.track)
    // Compute an aggregate function on a sample's track

    implicit def SampleFile2EIter(samples: SampleFile): SampleEIterator =
      samples.eiterator


  } // SFOpsWithImplicitAction


  def iaMaterialize(samples: => SampleFile) = {
    synchrony.iterators.FileCollections.newTMPDIR()
    samples.serialized
  }

  val AlwaysMaterialize = new SFOpsWithImplicitAction(iaMaterialize _)


  def iaSortIfNeeded(samples: => SampleFile) = {
    synchrony.iterators.FileCollections.newTMPDIR()
    new SampleFileParOps.SFOps(samples).tracksSortedIfNeeded
  }

  val AlwaysSortIfNeeded = new SFOpsWithImplicitAction(iaSortIfNeeded _)


} // End object SampleFileOpsImplicit 




