
package synchrony.gmql

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
 */


object SampleFileOpsImplicit {

  import synchrony.gmql.SampleFileOps 
  type SampleFile = SampleFileOps.SampleFile

  class SFOpsWithImplicitAction(mm: SampleFile => SampleFile) {

    val SampleFile       = SampleFileOps.SampleFile
    val Sample           = SampleFileOps.Sample
    val BedFileOps       = synchrony.genomeannot.BedFileOps 
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

      def tracksSorted: SampleFile = mm { 
        sf.tracksSorted
      }

      def tracksSortedIfNeeded: SampleFile =  mm { 
        sf.tracksSortedIfNeeded
      }

      def tracksAreSorted: Boolean = sf.tracksAreSorted
 
      def tracksSaved(nameOf: Sample => String): SampleFile = mm {
        sf.tracksSaved(nameOf)
      }

      def materialized: SampleFile = mm {
        samples
      }

      def toVector: Vector[Sample] = mm {
        samples
      }.toVector

      def onRegion(f: BedFile => BedFile): SampleFile = mm {
        sf.onRegion(f) 
      }

      def selectS(onSample: Sample => Boolean = (s: Sample) => true)
      : SampleFile = mm { 
        sf.selectS(onSample)
      }

      def projectS(onSample: (String, Sample => Any)*): SampleFile = mm { 
        sf.projectS(onSample: _*)
      }

      def extendS(onSample: (String, Sample => Any)*): SampleFile = mm { 
        sf.extendS(onSample: _*)
      }

      def groupbyS(
        grp: String, 
        aggr: (String, Aggr[Sample, Double])*
      ): SampleFile = mm {
        sf.groupbyS(grp, aggr: _*)
      }

      def differenceS
        (that: SampleFile)
        (implicit
           joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
           exact: Boolean = false)
      : SampleFile = mm {
        sf.differenceS(that)(joinby, exact)
      }

      def joinS
        (that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile,vs:BedFile)=>us,
         onSample: (Sample, Sample) => Sample = SFOut.overwrite,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : SampleFile = mm {
        sf.joinS(that)(onRegion, onSample, joinby)
      }

      def mapS
        (that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
         onSample: (Sample, Sample) => Sample = SFOut.left,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : SampleFile = mm {
        sf.mapS(that)(onRegion, onSample, joinby)
      }

      def coverS(
        onRegion: Seq[BedFile] => BedFile,
        onSample: String*)
      : SampleFile = mm {
        sf.coverS(onRegion, onSample: _*)
      }
    }


    object SFOps {

      def onRegion(samples: SampleFile)(f: BedFile=>BedFile): SampleFile = mm { 
        SFOpsExplicit.onRegion(samples)(f)
      }

      def selectS
        (samples: SampleFile)
        (onSample: Sample => Boolean = (s: Sample) => true)
      : SampleFile = mm {
        SFOpsExplicit.selectS(samples)(onSample)
      }

      def projectS
        (samples: SampleFile)
        (onSample: (String, Sample => Any)*)
      : SampleFile = mm {
        SFOpsExplicit.projectS(samples)(onSample: _*)
      }

      def extendS
        (samples: SampleFile)
        (onSample: (String, Sample => Any)*)
      : SampleFile = mm {
        SFOpsExplicit.extendS(samples)(onSample: _*)
      }

      def groupbyS
        (samples: SampleFile)
        (grp: String, 
         aggr: (String, Aggr[Sample, Double])*)
      : SampleFile = mm {
        SFOpsExplicit.groupbyS(samples)(grp, aggr: _*)
      }
    
      def differenceS
        (samples: SampleFile, that: SampleFile)
        (implicit
           joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true,
           exact: Boolean = false)
      : SampleFile = mm {
        SFOpsExplicit.differenceS(samples, that)(joinby, exact)
      }
    
      def joinS
        (samples: SampleFile, that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = (us:BedFile, vs:BedFile) => us,
         onSample: (Sample, Sample) => Sample = SFOut.overwrite,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : SampleFile = mm {
        SFOpsExplicit.joinS(samples, that)(onRegion, onSample, joinby)
      }
      
      def mapS
        (samples: SampleFile, that: SampleFile)
        (onRegion: (BedFile, BedFile) => BedFile = BFOps.mapR(),
         onSample: (Sample, Sample) => Sample = SFOut.left,
         joinby: (Sample, Sample) => Boolean = (s: Sample, t: Sample) => true)
      : SampleFile = mm {
        SFOpsExplicit.mapS(samples, that)(onRegion, onSample, joinby)
      }

      def coverS
        (samples: SampleFile)
        (onRegion: Seq[BedFile] => BedFile,
         onSample: String*)
      : SampleFile = mm {
        SFOpsExplicit.coverS(samples)(onRegion, onSample: _*)
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


  val AlwaysMaterialize = new SFOpsWithImplicitAction(
    (samples: SampleFile) => samples.serialized
  )

  val AlwaysSortIfNeeded = new SFOpsWithImplicitAction(
    (samples: SampleFile) => new SampleFileOps.SFOps(samples)
                                              .tracksSortedIfNeeded
  )

} // End object SampleFileOpsImplicit 


