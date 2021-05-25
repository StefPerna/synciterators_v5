

package synchrony.gmql

/** Provides simple representation of and operations on samples,
 *  modelled after GMQL.
 *
 * Wong Limsoon
 * 15 April 2021
 */


object Samples {

  import scala.reflect.{ classTag, ClassTag }
  import scala.io.Source
  import synchrony.genomeannot.BedWrapper
  import synchrony.iterators.FileCollections._
  import EFile._
  import synchrony.iterators.Serializers._



  //
  // Bed entries and BedFile form the main
  // component of a Sample. So put a copy
  // of these modules here for convenience.
  //

  type Meta = Map[String,Any]

  type LocusLike = BedWrapper.LocusLike
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

  case class ASample(override val meta:Meta, override val bedFile:BedFile)
  extends Sample


  trait Sample {

  /** An inherit class should supply the meta and bedFile.
   */

    val meta: Meta
    val bedFile: BedFile


  /** track is an EIterator on the BedFile.
   *  This lets you iterate on the BedFile many times.
   */

    def track: BedEIterator = bedFile.eiterator

    def slurpedTrack: BedEIterator = bedFile.slurpedEIterator


  /** trackSz is size of the BedFile.
   */

    def trackSz: Long = bedFile.filesize

   

  /** @param ps is a list of predicates.
   *  @param u is a Bed entry.
   *  @return whether predicates in ps all hold on this Sample and Bed entry u.
   */
  
    def cond(ps: (Sample, Bed) => Boolean*)(u: Bed): Boolean = {

      ps.forall(p => p(this, u))

    }


  /** @param ps is a list of predicates.
   *  @return whether predicates in ps all hold on this Sample.
   */

    def cond(ps: Sample => Boolean*): Boolean = ps.forall(p => p(this))



  /** @param f is the meta attribute.
   *  @return whether f is present in this Sample.
   */

    def hasM(f: String): Boolean = meta.contains(f)



  /** @param f is a meta attribute of this Sample.
   *  @return its value.
   */

    def getM[A](f:String): A = meta(f).asInstanceOf[A]

    def apply[A](f: String): A = getM(f)


  /** @param f is a meta attribute of this Sample.
   *  @param chk is a predicate on the value of f.
   *  @param whenNotFound is the value to return if f is not in this Sample.
   *  @return whether chk holds on f in this Sample.
   */

    def checkM[A](
      f:            String, 
      chk:          A => Boolean, 
      whenNotFound: Boolean = false): 
    Boolean = {

      meta.get(f) match { 
        case None => whenNotFound
        case Some(a) => chk(a.asInstanceOf[A])
      }

    }



  /** @param f is a meta attribute of this Sample.
   *  @param v is the new value for f.
   *  @return a copy of this Sample having this updated f.
   */

    def mUpdated(f: String, v: Any): Sample = {

      Sample(meta ++ Map(f -> v), bedFile)

    }



  /** @param kv is a list of meta attributes.
   *  @return a copy of this Sample updated with kv.
   */

    def mUpdated(kv: (String, Any)*): Sample = Sample(meta ++ kv, bedFile)

    def ++(kv: (String, Any)*): Sample = mUpdated(kv: _*)



  /** @param m is a Map[String,Any] of new meta attributes.
   *  @return the new Sample updated with these new meta attributes
   *     If there are conflicts, current meta attributes take priority.
   */

    def mMerged(m: Meta): Sample = Sample(m ++ meta, bedFile)



  /** @param kf is a function for producing meta attributes.
   *  @return a copy of this Sample merged with these meta attributes.
   */

    def mMergedWith(kf: (String, Sample => Any)*): Sample = {

      mMerged(Map(kf :_*).map { case (k, f) => k -> f(this) })

    }



  /** @param m is a Map[String,Any] of new meta attributes.
   *  @return the new Sample updated with these new meta attributes
   *     If there are conflicts, new meta attributes take priority.
   */

    def mOverwritten(m: Meta): Sample = Sample(meta ++ m, bedFile)



  /** @param kf is a function for producing meta attributes.
   *  @return a copy of this Sample overwritten by these meta attributes.
   */

    def mOverwrittenWith(kf: (String, Sample => Any)*): Sample = {

      mOverwritten(Map(kf :_*).map { case (k, f) => k -> f(this) } )

    }



  /** @param f is a meta attribute in this Sample.
   *  @return the new Sample with f deleted.
   */

    def mDeleted(f: String): Sample = Sample(meta - f, bedFile)



  /** @return a copy of this Sample with its meta attributes erased.
   */

    def mErased: Sample = Sample(meta.empty, bedFile)
    

  /** @param f is a renaming function.
   *  @return the new Sample with meta attributes renamed using f.
   */

    def mRenamed(f: String => String): Sample = {

      Sample(for ((k, v) <- meta) yield f(k) -> v, bedFile)

    }



  /** Sort the BedFile of this Sample using various orderings.
   *
   *  @param ordering is the ordering to use.
   *  @param cmp is the ordering to use.
   *  @return this Sample with its BedFile sorted using 
   *     the corresponding ordering.
   */

    def trackSorted: Sample =  Sample.sortTrack(this)


    def trackSortedWith(cmp: (Bed, Bed) => Boolean): Sample = {

      Sample.sortTrackWith(this)(cmp)

    }
    


  /** @param it is an iterator to produce a new BedFile.
   *  @return this Sample with new BedFile.
   */

    def trackUpdated(it: Iterator[Bed]): Sample = {

      bedFileUpdated(BedFile.transientBedFile(it).stored)

    }



    def trackUpdated(it: Iterable[Bed]): Sample = {

      bedFileUpdated(BedFile.transientBedFile(it.iterator).stored)

    }



  /** @param it is a new BedFile.
   *  @return this Sample with the new BedFile.
   */

    def bedFileUpdated(it:BedFile): Sample = Sample(meta, it)



  /** @param it is an iterator producing Bed entries.
   *  @return a copy of this Sample with its BedFile merged
   *     with these new Bed entries. Both list of Bed entries
   *     are assumed to be already sorted according to the
   *     default ordering.
   */

    def trackMerged(it:Iterator[Bed]): Sample = {

      bedFileUpdated(bedFile.mergedWith(BedFile.transientBedFile(it)).stored)

    }



  /** @param it is a BedFile.
   *  @return a copy of this Sample with its BedFile merged with
   *     this new BedFile. Both BedFiles are assumed to be already
   *     sorted in the default ordering.
   */

    def bedFileMerged(it:BedFile): Sample = {

      bedFileUpdated(bedFile.mergedWith(it).stored)

    }



  /** @return a copy of this Sample with region attributes erased
   *     from its BedFile.
   */

    def tmErased: Sample = {

      trackUpdated(for (r <- track) yield r.eraseMisc()) 

    }


  /** @param name is new name for this Sample's BedFile.
   *  @return a copy of this Sample with its BedFile renamed
   */

    def trackSavedAs(name: String, folder: String = ""): Sample = 
      Sample.saveTrackAs(this)(name, folder)


    def trackSerialized = Sample.serializeTrack(this)


  /** @return a copy of this Sample with meta attributes and region
   *     attributes erased.
   */

    def mAndTMErased(): Sample = this.mErased.tmErased

  }  // End class Sample



  /** Implementation of Sample methods.
   */

  object Sample {


  /** Construct Sample from BedFile.
   *
   *  @param meta is meta attributes
   *  @param bf is a Bed file
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, bf: BedFile): Sample = {

      new ASample(meta, bf)

    }


  /** Construct Sample from Iterator[Bed].
   *
   *  @param meta is meta attributes
   *  @param it is an iterator producing Bed entries
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, it: Iterator[Bed]): Sample = {

      Sample(meta, BedFile.transientBedFile(it).stored)

    }


  /** Construct Sample from Vector[Bed].
   *
   *  @param meta is meta attributes
   *  @param it is a vector of Bed entries
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, it: Vector[Bed]): Sample = {

      Sample(meta, BedFile.inMemoryBedFile(it))

    }


  /** Construct Sample from Bed file.
   *
   *  @param meta is meta attributes
   *  @param filename is name of the Bed file.
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, filename: String): Sample = {

      Sample(meta, BedFile.onDiskBedFile(filename))

    }


   /** A dummy ordering on Sample.
    *  It is needed for defining SampleFile later.
    */

    def dummyOrder: Ordering[Sample] = Ordering.by(_.getM[String]("sid"))


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

    def sortTrack(s: Sample): Sample = {

      Sample(s.meta, s.bedFile.sorted.stored)

    }

    

    def sortTrackWith(s: Sample)(cmp: (Bed, Bed) => Boolean): Sample = {

      Sample(s.meta, s.bedFile.sortedWith(cmp = cmp).stored)

    }


    def saveTrackAs
      (s: Sample)
      (implicit name: String = "", folder: String = "")
    : Sample = {
      
      Sample(s.meta,
             name match {
               case "" => s.bedFile.serialized(folder)
               case _  => s.bedFile.savedAs(name, folder)
            } )

    }


    def serializeTrack(s: Sample)(implicit folder: String = "")
    : Sample = saveTrackAs(s)(name = "", folder = folder)

  }  // End object Sample



  //
  // Define basic serializers and deserializers for Sample files.
  //
 

  object BaseSampleFileSerializers {

  /** Formatter.
   *
   *  Format a sample for serialization in Limsoon's format.
   *
   *  @param e is a Sample.
   *  @return a String representing e.
   */

    case object SampleFormatter extends Formatter[Sample] {

      override def format(e: Sample): String = {
        // The track of e must be serialized first, so that the
        // track is represented by its filename.
        val sid = e.bedFile.serialized.filename
        // The meta attributes of e are on a tap-delimited line each.
        val fields = for((f, v) <- e.meta) yield s"${f}\t${v}\n"
        // Return the two pieces of info above in one String.
        s"${sid}\n${fields.mkString}"
      }
    }  // End object SampleFormatter.



  /** Default serializer for Samples.
   */

    val serializerSampleFile: Serializer[Sample] =  
      EFile.serializerEFile[Sample](SampleFormatter)


  /** Constructor for a customizable Parser for Sample, Limsoon's format.
   *
   *  Sample has a track, which is a Bed file. The optional fields of
   *  BedFile can be named and interpreted differently in different
   *  subformats. So need to provide support for customization to
   *  various subformats.
   *
   *  @val customization defines the subformat.
   *  @return            the customized Parser.
   */

    case class CustomizableSampleParser(
      sampleParser: Parser[Sample],
      customization: Sample => Sample)
    extends Parser[Sample] {

      override def parse(ois: EIterator[String]): Sample = {
        // Use the base SampleParser to construct an uncustomized Sample.
        val sample = sampleParser.parse(ois)
        // Customize the Sample as needed.
        (customization == null) match {
          case true  => sample
          case false => customization(sample)
        }
      }

      def customize(newCustomization: Sample => Sample)
      : CustomizableSampleParser = {
        new CustomizableSampleParser(sampleParser, newCustomization)
      }
    }

  
  /** Constructor for a base Parser for Sample, with no customization,
   *  in Limsoon's format.
   */

    class SampleParser(onDiskBedFile: String => BedFile)
    extends Parser[Sample] {

  /** @param ois is an EIterator[String] representing a Sample.
   *  @return    the parsed Sample.
   */

      override def parse(ois: EIterator[String]): Sample = {
        // The first non-blank line is path to the Sample's Bed file.
        var sid = ois.next().trim
        while (sid == "") { sid = ois.next().trim }
        // All following lines, until the next blank line, are
        // the meta attributes of this Sample.
        val lines = ois.takeWhile(_.trim != "").toVector
        val fields =
          for(l <- lines;
              m = l.trim.split("\t");
              m0: String = m(0);
              m1: Any = try m(1).toInt    catch { case _: Throwable =>
                        try m(1).toDouble catch { case _: Throwable =>
                        m(1) } } ) 
          yield (m0, m1)
        val meta = Map[String, Any](fields.toSeq :_*)
        Sample(meta, onDiskBedFile(sid)) 
      }

      def customize(newCustomization: Sample => Sample)
      : CustomizableSampleParser = {
        new CustomizableSampleParser(this, newCustomization)
      }
    }  // End SampleParser.


    object BaseSampleParser
    extends SampleParser(sid => BedFile.onDiskBedFile(sid))


    val deserializerSampleFile: Deserializer[Sample] = {
      EFile.deserializerEFile[Sample](BaseSampleParser)
    }


   val defaultSettingsSampleFile = 
     EFile.setDefaultsEFile[Sample](
       suffixtmp    = ".sftmp",
       suffixsav    = ".sfsav",
       serializer   = serializerSampleFile,
       deserializer = deserializerSampleFile
     )(classTag[Sample], Sample.dummyOrder)

  }  // End object BaseSampleFileSerializers 


}


