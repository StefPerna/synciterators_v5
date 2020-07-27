

package synchrony.gmql

/** Provides simple representation of and operations on samples,
 *  modelled after GMQL.
 *
 * Wong Limsoon
 * 27 April 2020
 */


object Samples {

  import java.io.{ File, PrintWriter }
  import scala.io.Source
  import synchrony.genomeannot.BedWrapper
  import synchrony.iterators.FileCollections.{
    EFile, EFileState, EFileSettings, EIterator,
    InMemory, Transient, OnDisk, FileNotFound
  }



  //
  // Bed entries and BedFile form the main
  // component of a Sample. So put a copy
  // of these modules here for convenience.
  //

  type Meta = Map[String,Any]

  type Bed = BedWrapper.Bed
  val Bed = BedWrapper.SimpleBedEntry

  type BedFile = BedWrapper.BedFile
  val BedFile = BedWrapper.BedFile

  type BedEIterator = BedWrapper.BedEIterator
  val BedEIterator = EIterator
  

  /** Sample has meta attributes and has a BedFile associated with it.
   *
   *  @param meta is a Map[String,Any], representing the meta attributes.
   *  @param bedFile is the BedFile.
   */

  case class Sample(meta:Meta, bedFile:BedFile) {


  /** track is an EIterator on the BedFile.
   *  This lets you iterate on the BedFile many times.
   */

    def track: BedEIterator = Sample.trackOf(this)


  /** trackSz is size of the BedFile.
   */

    def trackSz: Long = Sample.trackSzOf(this)

   

  /** @param ps is a list of predicates.
   *  @param u is a Bed entry.
   *  @return whether predicates in ps all hold on this Sample and Bed entry u.
   */
  
    def cond(ps: (Sample, Bed) => Boolean*)(u: Bed): Boolean = {

      Sample.cond(ps:_*)(this, u)

    }


  /** @param ps is a list of predicates.
   *  @return whether predicates in ps all hold on this Sample.
   */

    def cond(ps: Sample => Boolean*): Boolean = Sample.cond(ps:_*)(this)



  /** @param f is the meta attribute.
   *  @return whether f is present in this Sample.
   */

    def hasM(f: String): Boolean = Sample.hasM(this, f)


  /** @param f is a meta attribute of this Sample.
   *  @return its value.
   */

    def getM[A](f:String): A = Sample.getM[A](this, f)


  /** @param f is a meta attribute of this Sample.
   *  @param chk is a predicate on the value of f.
   *  @param whenNotFound is the value to return if f is not in this Sample.
   *  @return whether chk holds on f in this Sample.
   */

    def checkM[A](
      f:            String, 
      chk:          A => Boolean, 
      whenNotFound: Boolean = false)
    : Boolean = {

      Sample.checkM[A](this, f, chk, whenNotFound)

    }



  /** @param f is a meta attribute of this Sample.
   *  @param v is the new value for f.
   *  @return a copy of this Sample having this updated f.
   */

    def mUpdated(f: String, v: Any): Sample = Sample.updateM(this, (f, v))


  /** @param kv is a list of meta attributes.
   *  @return a copy of this Sample updated with kv.
   */

    def mUpdated(kv: (String, Any)*): Sample = Sample.updateM(this, kv:_*)


  /** @param m is a Map[String,Any] of new meta attributes.
   *  @return the new Sample updated with these new meta attributes
   *     If there are conflicts, current meta attributes take priority.
   */

    def mMerged(m: Meta): Sample = Sample.mergeM(this, m)


  /** @param kf is a function for producing meta attributes.
   *  @return a copy of this Sample merged with these meta attributes.
   */

    def mMergedWith(kf: (String, Sample => Any)*): Sample = {

      Sample.mergeMWith(this, kf:_*)

    }



  /** @param m is a Map[String,Any] of new meta attributes.
   *  @return the new Sample updated with these new meta attributes
   *     If there are conflicts, new meta attributes take priority.
   */

    def mOverwritten(m: Meta): Sample = Sample.overwriteM(this, m)


  /** @param kf is a function for producing meta attributes.
   *  @return a copy of this Sample overwritten by these meta attributes.
   */

    def mOverwrittenWith(kf: (String, Sample => Any)*): Sample = {

      Sample.overwriteMWith(this, kf:_*)

    }



  /** @param f is a meta attribute in this Sample.
   *  @return the new Sample with f deleted.
   */

    def mDeleted(f: String): Sample = Sample.delM(this, f)


  /** @return a copy of this Sample with its meta attributes erased.
   */

    def mErased: Sample = Sample.eraseM(this)
    

  /** @param f is a renaming function.
   *  @return the new Sample with meta attributes renamed using f.
   */

    def mRenamed(f: String => String): Sample = Sample.renameM(this, f)


  /** Sort the BedFile of this Sample using various orderings.
   *
   *  @param ordering is the ordering to use.
   *  @param cmp is the ordering to use.
   *  @return this Sample with its BedFile sorted using 
   *     the corresponding ordering.
   */

    def trackSorted
      (implicit ordering: Ordering[Bed] = Sample.ordering)
    : Sample = {

      Sample.sortTrack(this)(ordering) 

    }


    def trackSortedWith(cmp: (Bed, Bed) => Boolean): Sample = {

      Sample.sortTrackWith(this)(cmp)

    }
    


  /** @param it is an iterator to produce a new BedFile.
   *  @return this Sample with new BedFile.
   */

    def trackUpdated(it: Iterator[Bed]): Sample = Sample.updateTrack(this, it)

    def trackUpdated(it: Iterable[Bed]): Sample = {

      Sample.updateTrack(this, it.iterator)

    }



  /** @param it is a new BedFile.
   *  @return this Sample with the new BedFile.
   */

    def bedFileUpdated(it:BedFile): Sample = Sample.updateBedFile(this, it)


  /** @param it is an iterator producing Bed entries.
   *  @return a copy of this Sample with its BedFile merged
   *     with these new Bed entries. Both list of Bed entries
   *     are assumed to be already sorted according to the
   *     default ordering.
   */

    def trackMerged(it:Iterator[Bed]): Sample = Sample.mergeTrack(this, it)


  /** @param it is a BedFile.
   *  @return a copy of this Sample with its BedFile merged with
   *     this new BedFile. Both BedFiles are assumed to be already
   *     sorted in the default ordering.
   */

    def bedFileMerged(it:BedFile): Sample = Sample.mergeBedFile(this, it)


  /** @return a copy of this Sample with region attributes erased
   *     from its BedFile.
   */

    def tmErased(): Sample = Sample.eraseTM(this)


  /** @return a copy of this Sample with meta attributes and region
   *     attributes erased.
   */

    def mAndTMErased(): Sample = Sample.eraseMandTM(this)

  }  // End class Sample



  /** Implementation of Sample methods.
   */

  object Sample {


  /** Construct Sample from Iterator[Bed].
   *
   *  @param meta is meta attributes
   *  @param it is an iterator producing Bed entries
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, it: Iterator[Bed]): Sample = {

      new Sample(meta, BedFile.transientBedFile(it).stored)

    }



  /** Construct Sample from Vector[Bed].
   *
   *  @param meta is meta attributes
   *  @param it is a vector of Bed entries
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, it: Vector[Bed]): Sample = {

      new Sample(meta, BedFile.inMemoryBedFile(it))

    }



  /** Construct Sample from Bed file.
   *
   *  @param meta is meta attributes
   *  @param filename is name of the Bed file.
   *  @param customization is the customization for optional fields in the file.
   *  @return the constructed Sample.
   */

    def apply(
      meta: Meta, 
      filename: String, 
      customization: Bed => Bed = (x: Bed) => x)
   : Sample = {

     new Sample(meta, BedFile.onDiskBedFile(filename, customization))

    }

  

  /** Construct Sample from ENCODE Narrow Peak file.
   *
   *  @param meta is meta attributes
   *  @param filename is name of the Bed file.
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, filename: String): Sample = {

      new Sample(meta, BedFile.onDiskEncodeNPBedFile(filename))

    }



  /** @param s is a Sample.
   *  @return an EIterator on the BedFile of Sample s.
   */

    def trackOf(s: Sample): BedEIterator = s.bedFile.eiterator


  /** @param s is a Sample.
   *  @return size of the BedFile of Sample s.
   */

    def trackSzOf(s: Sample): Long = s.bedFile.filesize


  /** @param ps is a list of predicates.
   *
   *  @param s is a Sample
   *  @param u is a Bed entry.
   *  @return whether predicates in ps all hold on Sample s and Bed entry u.
   */
  
    def cond(ps: (Sample, Bed) => Boolean*)(s:Sample, u:Bed): Boolean = {

      ps.forall(p => p(s,u))

    }



  /** @param ps is a list of predicates.
   *
   *  @param s is a Sample
   *  @return whether predicates in ps all hold on Sample s.
   */

    def cond(ps: Sample => Boolean*)(s:Sample): Boolean = ps.forall(p => p(s))



  /** @param s is a Sample.
   *  @param f is a meta attribute.
   *  @return whether f is present in Sample s.
   */

    def hasM(s: Sample, f: String): Boolean = s.meta.contains(f)


  /** @param s is a Sample.
   *  @param f is a meta attribute of Sample s.
   *  @return its value.
   */

    def getM[A](s: Sample, f:String): A = s.meta(f).asInstanceOf[A]


  /** @param s is a Sample.
   *  @param f is a meta attribute of s.
   *  @param chk is a predicate on the value of f.
   *  @param whenNotFound is the value to return if f is not in Sample s.
   *  @return whether chk holds on f in this Sample.
   */

    def checkM[A](
      s:            Sample,
      f:            String, 
      chk:          A => Boolean, 
      whenNotFound: Boolean = false)
    : Boolean = {

      s.meta.get(f) match { 
        case None => whenNotFound
        case Some(a) => chk(a.asInstanceOf[A])
      }

   }



  /** @param s is a Sample.
   *  @param kv is a list of meta attributes
   *  @return a copy of Sample s updated by kv.
   */

    def updateM(s: Sample, kv: (String, Any)*): Sample = {

      new Sample(s.meta ++ kv, s.bedFile) 

    }



  /** @param s is a Sample.
   *  @param m is a Map[String,Any] of new meta attributes.
   *  @return a copy Sample s updated with these new meta attributes
   *     If there are conflicts, current meta attributes take priority.
   */

    def mergeM(s: Sample, m: Meta): Sample = new Sample(m ++ s.meta, s.bedFile)



  /** @param s is a Sample.
   *  @param kf is a function producing new meta attributes.
   *  @return a copy of Sample s merged with these new meta attributes.
   */

    def mergeMWith(s: Sample, kf: (String, Sample => Any)*): Sample = {

      mergeM(s, Map(kf:_*).map { case (k, f) => k -> f(s) })

    }



  /** @param s is a Sample.
   *  @param m is a Map[String,Any] of new meta attributes.
   *  @return a copy of Sample s updated with these new meta attributes
   *     If there are conflicts, new meta attributes take priority.
   */

    def overwriteM(s: Sample, m: Meta): Sample = {

      new Sample(s.meta ++ m, s.bedFile)

    }



  /** @param s is a sample.
   *  @param kf is a function producing new meta attributes.
   *  @return a copy of Sample s overwritten with these new meta attributes.
   */
 
    def overwriteMWith(s: Sample, kf:(String, Sample => Any)*): Sample = {

      overwriteM(s, Map(kf:_*).map { case (k, f) => k -> f(s) })

    }



  /** @param s is a Sample.
   *  @param f is a meta attribute in this Sample.
   *  @return a copy of Sample s with f deleted.
   */

    def delM(s: Sample, f: String): Sample = {

      new Sample(s.meta - f, s.bedFile)

    }



  /** @param s is a Sample.
   *  @return a copy of Sample s with no meta attributes.
   */

    def eraseM(s: Sample): Sample = new Sample(s.meta.empty, s.bedFile)
    

  /** @param s is a Sample.
   *  @param f is a renaming function.
   *  @return a copy of Sample s with meta attributes renamed using f.
   */

    def renameM(s: Sample, f: String => String): Sample = {

      new Sample(for ((k, v) <- s.meta) yield f(k)->v, s.bedFile)

    }




  /** Ordering on Bed entries, repeated here for convenience.
   */

    val ordering: Ordering[Bed]          = Bed.ordering
    val orderByChromEnd: Ordering[Bed]   = Bed.orderByChromEnd
    val orderByChromStart: Ordering[Bed] = Bed.orderByChromStart


  /** Sort the BedFile of a Sample using various orderings.
   *
   *  @param s is the Sample
   *  @param ordering is the ordering to use.
   *  @param cmp is the ordering to use.
   *  @return this Sample with its BedFile sorted using 
   *     the corresponding ordering.
   */

    def sortTrack
      (s: Sample)
      (implicit ordering: Ordering[Bed] = ordering)
    : Sample = {

      new Sample(s.meta, s.bedFile.sorted(cmp = ordering).stored)

    }

    

    def sortTrackWith(s: Sample)(cmp: (Bed, Bed) => Boolean): Sample = {

      new Sample(s.meta, s.bedFile.sortedWith(cmp = cmp).stored)

    }

    
    

  /** @param s is a Sample.
   *  @param it is an iterator to produce a new BedFile.
   *  @return Sample s having its BedFile replaced by this new BedFile.
   */

    def updateTrack(s: Sample, it: Iterator[Bed]): Sample = {

      updateBedFile(s, BedFile.transientBedFile(it).stored)

    }



  /** @param s is a Sample. 
   *  @param it is a new BedFile.
   *  @return Sample s having its BedFile replaced by the new BedFile.
   */

    def updateBedFile(s: Sample, it:BedFile): Sample =  new Sample(s.meta, it)



  /** @param s is a Sample
   *  @param it is an iterator producing Bed entries.
   *  @return a copy of Sample s having its BedFile merged
   *     with these new Bed entries. Both list of Bed entries
   *     are assumed to be already sorted according to the
   *     default ordering.
   */

    def mergeTrack(s: Sample, it: Iterator[Bed]): Sample = {

      mergeBedFile(s, BedFile.transientBedFile(it))

    }



  /** @param s is a Sample
   *  @param it is a BedFile.
   *  @return a copy of Sample s having its BedFile merged with
   *     this new BedFile. Both BedFiles are assumed to be already
   *     sorted in the default ordering.
   */

    def mergeBedFile(s: Sample, it: BedFile): Sample = {

      updateBedFile(s, s.bedFile.mergedWith(it).stored)

    }



  /** @param s is a Sample.
   *  @return a copy of Sample s with region attributes erased.
   */

    def eraseTM(s: Sample): Sample = {

      updateTrack(s, for (r <- s.track) yield r.eraseMisc()) 

    }



  /** @param s is a Sample.
   *  @return a copy of Sample s with meta attributes and region
   *     attributes erased.
   */

    def eraseMandTM(s: Sample): Sample = eraseTM(eraseM(s))

  }  // End object Sample




  /**
   * Set up customized EIterator and EFile for Sample.
   * Needed because SampleFile has its special formats on disk.
   */

  type SampleEIterator = EIterator[Sample]

  type SampleFile = EFile[Sample]

  type SampleFileSettings = EFileSettings[Sample]


  object SampleFile {


  /** OpG provides many commonly used aggregate functions. 
   *  Put a copy here for convenience.
   */

    val OpG = synchrony.iterators.AggrCollections.OpG



  /** Default settings for SampleFile. 
   *
   *  Change file suffixes to ".sftmp" and ".sfsav".
   *  Change serializer and deserializer to the ones
   *  customized for SampleFile.
   */

    val defaultSettingsSampleFile = EFile.setDefaultsEFile[Sample](
      suffixtmp       = ".sftmp",
      suffixsav       = ".sfsav",
      aveSz           = 1000,
      cap             = 70000,
      totalsizeOnDisk = totalsizeOnDiskSampleFile _,
      serializer      = defaultSerializerSampleFile _,
      deserializer    = defaultDeserializerSampleFile _
    )


    def altSettingsSampleFile(dbpath: String) = EFile.setDefaultsEFile[Sample](
      suffixtmp       = ".sftmp",
      suffixsav       = ".sfsav",
      aveSz           = 1000,
      cap             = 70000, 
      totalsizeOnDisk = totalsizeOnDiskSampleFile _,
      serializer      = defaultSerializerSampleFile _,
      deserializer    = altDeserializerSampleFile(dbpath) _
    )


    def nullCustomizationSampleFile(x: Sample) = x

    def toEncodeNPSampleFile(x: Sample): Sample = x.bedFile.efile match
    {
      // The Bed file should be on disk.

      case bf: OnDisk[Bed] => 
        x.bedFileUpdated(BedFile.onDiskEncodeNPBedFile(bf.filename))

      // It is not on disk; do nothing.

      case _ => x
    }


  /** altDeserializerSampleFile is the deserializer for reading
   *  sample files prepared by Stefano Perna.
   *  
   *  Samples are assumed to be kept in a sample folder (dbpath).
   *  Each sample is assumed to be kept in two files, one for its
   *  associated bed file (sid), one  for its associated meta data
   *  (sid.meta). And there is a file listing the sid of samples 
   *  to be deserialized.
   *
   *  @param path is the folder where Samples are kept.
   *  @param customization is the customization needed for optional Bed fields.
   *  @param samplelist is the list of Samples to deserialize.
   *  @return a SampleEIterator on the deserialized Samples.
   */
         
    def altDeserializerSampleFile
      (path: String)
      (customization: Sample => Sample = nullCustomizationSampleFile _)
    = (samplelist: String) => {

      def openFile(f: String) = {
        val file  = 
          try Source.fromFile(f) 
          catch { case _ : Throwable => throw FileNotFound(f) }
        try file.getLines.toVector.iterator finally file.close()
      }

      def closeFile(ois: Iterator[String]) = { }

      def parseSample
        (path: String, customization: Sample => Sample)
        (ois: Iterator[String]) = {

        val sid = ois.next().trim
        val mf = s"${path}/${sid}.meta"
        val bf = s"${path}/${sid}"

        val metafile = try Source.fromFile(mf)
                       catch { case _ : Throwable => throw FileNotFound(mf) }

        val fields = for (l <- metafile.getLines; e = l.trim.split("\t")) 
                     yield (e(0) -> e(1))

        try customization(
              Sample(Map(fields.toSeq :_*) + ("sid" -> sid),
                     BedFile.onDiskBedFile(bf)))
        finally metafile.close() 
      }

      if ((samplelist endsWith defaultSettingsSampleFile.suffixtmp) ||
          (samplelist endsWith defaultSettingsSampleFile.suffixsav)) { 

        // Hey, the file is in Limsoon's format,
        // use Limsoon's deserializer.
      
        defaultDeserializerSampleFile(customization)(samplelist)
      }

      else { 

        // OK, the file is in Stefano's format.

        EIterator.makeParser(
          openFile,
          parseSample(path, customization),
          closeFile)(samplelist)
      }
    }



  /** defaultDeserializerSampleFile is deserializer for 
   *  Limsoon's prefered format for representing samples.
   *
   *  All the samples are in one file, separated by single
   *  blank lines. Each meta attribute is on separate line,
   *  and has the form fieldname \t fieldvalue.  Moreover,
   *  the first line is always the filename of the
   *  associated Bed file.
   *
   *  @param customization is the customization for optionalk fields in Bed.
   *  @param samplelist is the list of Samples to deserialize.
   *  @return a SampleEIterator on the deserialized Samples.
   */

    def defaultDeserializerSampleFile
        (customization: Sample => Sample)
        (samplelist: String)
    : SampleEIterator = {

      def openFile(f:String) = {
        val file = Source.fromFile(f)
        val lines = file.getLines.map(_.trim).toVector.iterator
        file.close()
        lines
      }

      def closeFile(ois:Iterator[String]) = { }

      def parseSample
          (customization:Sample => Sample)
          (ois: Iterator[String]) = {

        val sid = ois.next()

        val lines = ois.takeWhile(_ != "").toVector

        val meta: Meta = {
          val fields =
            for(l <- lines; m = l.split("\t")) yield {
              val m0:String = m(0)
              val m1:Any = try m(1).toDouble catch { case _:Throwable => m(1) } 
              (m0, m1)
            }
          Map[String, Any](fields.toSeq :_*)
        } 

        customization(Sample(meta, BedFile.onDiskBedFile(sid))) 
      }

      EIterator.makeParser(openFile, 
                           parseSample(customization),
                           closeFile)(samplelist) 
    }



  /** Serialize Samples to a file.
   *
   *  @param it is a SampleEIterator.
   *  @param filename is name of the output file.
   */

    def defaultSerializerSampleFile(it: SampleEIterator)(filename: String) = {

      val oos = new PrintWriter(new File(filename))

      for (e <- it) {
        val sid = e.bedFile.serialized.filename
        oos.write(s"${sid}\n")
        for((f, v) <- e.meta) oos.write(s"${f}\t${v}\n")
        oos.write("\n")
        oos.flush()
      }

      oos.flush()
      oos.close()
    }



  /** Construct SampleFile from EFileState 
   *
   *  @param efile is an EFileState.
   *  @return the constructed SampleFile.
   */
   
    def apply(efile: EFileState[Sample]): SampleFile = new SampleFile(efile)


  /** Construct in-memory SampleFile.
   *
   *  @param entries is a vector of Samples.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */

    def inMemorySampleFile(
      entries: Vector[Sample],
      settings: EFileSettings[Sample] = defaultSettingsSampleFile)
    : SampleFile = {

        apply(InMemory(entries, settings))

    }
    


  /** Construct transient SampleFile.
   *
   *  @param entries is an iterator of Samples.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */

    def transientSampleFile(
      entries: Iterator[Sample],
      settings: EFileSettings[Sample] = defaultSettingsSampleFile)
    : SampleFile = {

      apply(Transient(entries, settings))

    }

    

  /** Construct on-disk SampleFile.
   *
   *  @param filename is name of the SampleFile.
   *  @param customization is customization for optional fields.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */
  
    def onDiskSampleFile(
      filename: String,
      customization: Sample => Sample = nullCustomizationSampleFile _,
      settings: EFileSettings[Sample] = defaultSettingsSampleFile)
    : SampleFile = {

      apply(OnDisk[Sample](filename, customization, settings))
    
    }


  /** Construct an on-disk SampleFile from files created by Stefano Perna.
   *
   *  @param dbpath is a folder of Samples.
   *  @param samplelist is list of Samples to deserialize.
   *  @param customization is customization for optional fields.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */

    def altOnDiskSampleFile
      (dbpath: String) 
      (samplelist: String,
       customization: Sample => Sample = nullCustomizationSampleFile _,
       settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
    : SampleFile = {

      apply(OnDisk[Sample](samplelist, customization, settings))

    }

    

  /** Construct an on-disk SampleFile from files created by Stefano Perna,
   *  customize their Bed entries as ENCODE Narrow Peaks. 
   *
   *  @param dbpath is a folder of Samples.
   *  @param samplelist is list of Samples to deserialize.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */

    def altOnDiskEncodeNPSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
    : SampleFile = {

      apply(OnDisk[Sample](samplelist, toEncodeNPSampleFile _, settings))

    }



  /** @param efobj is a SampleFile.
   *  @return the total size of efobj if it is on disk,
   *     inclusive of all its component files.
   */
 
    def totalsizeOnDiskSampleFile(efobj: SampleFile):Double = 
      efobj.efile match {

        case ef: Transient[Sample] => 0

        case ef: InMemory[Sample] => {
          import synchrony.iterators.FileCollections.implicits._
          import synchrony.iterators.AggrCollections.OpG.sum
          efobj.processedWith[Double](
            _.flatAggregateBy(sum[Sample](_.bedFile.totalsizeOnDisk))
          )
        }

        case ef: OnDisk[Sample] => {
          import synchrony.iterators.FileCollections.implicits._
          import synchrony.iterators.AggrCollections.OpG.sum
          efobj.filesize +
          efobj.processedWith[Double](
            _.flatAggregateBy(sum[Sample](_.bedFile.totalsizeOnDisk))
          )
        }
      }

  }  // End object SampleFile


  // Put a copy of FileCollections.implicits here for convenience.

  val implicits = synchrony.iterators.FileCollections.implicits

}  // End object Samples.


/**
 * Here are examples on using SampleFile...
 *

   {{{

import synchrony.gmql.Samples._
import synchrony.gmql.Samples.SampleFile._
import synchrony.gmql.Samples.SampleFile.OpG._

val dir = "../../synchrony-1/test/test-massive/"
val ctcfPath = dir + "cistrome_hepg2_narrowpeak_ctcf/files"
val ctcfList = dir + "ctcf-list.txt"

// ctcfFiles is an EFile of samples.
// i.e. ctcfFiles.eiterator is an iterator on the ctcf samples.

val ctcfFiles = altOnDiskEncodeNPSampleFile(ctcfPath)(ctcfList)
// val ctcfFiles = altOnDiskSampleFile(ctcfPath)(ctcfList)

ctcfFiles.eiterator.toVector

ctcfFiles.eiterator.length


ctcfFiles(2).bedFile(10) 
// get the 10th bed entry on the 2nd sample's track

val bf = Sample(
  Map("dummy" -> 123),
  for(x <- ctcfFiles(2).bedFile.eiterator; if (x.chrom == "chr1")) yield x 
)


bf.bedFile(10)

bf.getM[Int]("dummy")


// import synchrony.iterators.AggrCollections.OpG._

bf.bedFile.flatAggregateBy(biggest(_.score))

ctcfFiles.flatAggregateBy(count)

    }}}

 *
 *
 */



