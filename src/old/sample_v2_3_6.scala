

package synchrony.gmql

/** Provides simple representation of and operations on samples,
 *  modelled after GMQL.
 *
 * Wong Limsoon
 * 29 September 2020
 */


object Samples {

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

  case class Sample(meta:Meta, bedFile:BedFile) {


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

      new Sample(meta ++ Map(f -> v), bedFile)

    }



  /** @param kv is a list of meta attributes.
   *  @return a copy of this Sample updated with kv.
   */

    def mUpdated(kv: (String, Any)*): Sample = new Sample(meta ++ kv, bedFile)

    def ++(kv: (String, Any)*): Sample = mUpdated(kv: _*)



  /** @param m is a Map[String,Any] of new meta attributes.
   *  @return the new Sample updated with these new meta attributes
   *     If there are conflicts, current meta attributes take priority.
   */

    def mMerged(m: Meta): Sample = new Sample(m ++ meta, bedFile)



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

    def mOverwritten(m: Meta): Sample = new Sample(meta ++ m, bedFile)



  /** @param kf is a function for producing meta attributes.
   *  @return a copy of this Sample overwritten by these meta attributes.
   */

    def mOverwrittenWith(kf: (String, Sample => Any)*): Sample = {

      mOverwritten(Map(kf :_*).map { case (k, f) => k -> f(this) } )

    }



  /** @param f is a meta attribute in this Sample.
   *  @return the new Sample with f deleted.
   */

    def mDeleted(f: String): Sample = new Sample(meta - f, bedFile)



  /** @return a copy of this Sample with its meta attributes erased.
   */

    def mErased: Sample = new Sample(meta.empty, bedFile)
    

  /** @param f is a renaming function.
   *  @return the new Sample with meta attributes renamed using f.
   */

    def mRenamed(f: String => String): Sample = {

      new Sample(for ((k, v) <- meta) yield f(k) -> v, bedFile)

    }



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

    def trackUpdated(it: Iterator[Bed]): Sample = {

      bedFileUpdated(BedFile.transientBedFile(it).stored)

    }



    def trackUpdated(it: Iterable[Bed]): Sample = {

      bedFileUpdated(BedFile.transientBedFile(it.iterator).stored)

    }



  /** @param it is a new BedFile.
   *  @return this Sample with the new BedFile.
   */

    def bedFileUpdated(it:BedFile): Sample = new Sample(meta, it)



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

    def tmErased(): Sample = {

      trackUpdated(for (r <- track) yield r.eraseMisc()) 

    }


  /** @param name is new name for this Sample's BedFile.
   *  @return a copy of this Sample with its BedFile renamed
   */

    def trackSavedAs(name: String): Sample = Sample.saveTrackAs(this)(name)



  /** @return a copy of this Sample with meta attributes and region
   *     attributes erased.
   */

    def mAndTMErased(): Sample = this.mErased.tmErased

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
   *  @return the constructed Sample.
   */

    def apply(meta: Meta, filename: String): Sample = {

      new Sample(meta, BedFile.onDiskBedFile(filename))

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


    def saveTrackAs(s: Sample)(name: String): Sample = {
      
      new Sample(s.meta, s.bedFile.savedAs(name))

    }


  }  // End object Sample





  //
  // Define serializers and deserializers for Sample files.
  //
  // This is a bit complicated because Sample files are in two
  // different formats, Limsoon's and Stefano's; and we try to 
  // handle these transparently.
  //
 

  object SampleFileSerializers {


  /** Serializer for Sample files.
   *
   *  Serialization uses only Limsoon's format for representing
   *  Sample. This format is more convenient as all the samples
   *  data are kept in one file.
   *
   *  Specifically: All the samples are in one file, separated 
   *  by single blank lines. Each meta attribute is on separate
   *  line, and has the form fieldname \t fieldvalue.  Moreover,
   *  the first line of each entry is always the filename of the 
   *  associated Bed file.
   *
   *  This format is more "robust" and efficient than Stefano's.
   *  Stefano's format, each sample's info is stored in a separate file.
   *  This means it is very slow to read them, as extra system
   *  calls are needed to read them. Worse, to read them, a user
   *  is provided with a second piece of info: a list of sample id's.
   *  But there is no file path in that list! The file path is 
   *  provided as a third piece of info in another separate input.
   *  If you lose any of the three pieces of info, you have lost
   *  your data.
   *
   *  So now you know why we only serialize Sample files into 
   *  Limsoon's format.
   */


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

    val serializerSampleFile: Serializer[Sample] = { 

      EFile.serializerEFile[Sample](SampleFormatter)

    }


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

    case class CustomizableSampleParser(customization: Sample => Sample)
    extends Parser[Sample] {

      override def parse(ois: EIterator[String]): Sample = {

        // Use the base SampleParser to construct an uncustomized Sample.
        val sample = SampleParser.parse(ois)

        // Customize the Sample as needed.
        (customization == null) match {
          case true  => sample
          case false => customization(sample)
        }
      }

      def customize(newCustomization: Sample => Sample):
      CustomizableSampleParser = {
        new CustomizableSampleParser(newCustomization)
      }
    }

  
  /** Constructor for a base Paser for Sample, with no customization,
   *  in Limsoon's format.
   *
   */

    case object SampleParser extends Parser[Sample] {


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
        val meta: Meta = {
          val fields =
            for(l <- lines;
                m = l.trim.split("\t");
                m0: String = m(0);
                m1: Any = try m(1).toDouble catch { case _:Throwable => m(1) }) 
            yield (m0, m1)
          Map[String, Any](fields.toSeq :_*)
        } 

        // Return the parsed Sample.
        Sample(meta, BedFile.onDiskBedFile(sid)) 
      }

      def customize(newCustomization: Sample => Sample):
      CustomizableSampleParser = {
        new CustomizableSampleParser(newCustomization)
      }

    }  // End object SampleParser.



  /** Constructor for an alternative customizable Parser, for
   *  Sample files in Stefano's format.
   *
   *  This alternative format is quite inconvenient as the main
   *  Sample file is just a list of sample names. The actual path
   *  to a folder containing the files are provided separately.
   *
   *  @param customization defines the customization needed.
   *  @param path          points to the folder where the samples are.
   *  @return              a customized parser.
   */

    case class AltCustomizableSampleParser(
      customization: Sample => Sample, 
      path: String)
    extends Parser[Sample] {

      override def parse(ois: EIterator[String]): Sample = {

        // Use the alternative base parser to construct an uncustomized Sample.
        val sample = AltSampleParser.parse(path)(ois)

        // Customize the parsed sample as needed.
        (customization == null) match {
          case true  => sample
          case false => customization(sample)
        }
      }


      def customize(newCustomization: Sample => Sample):
      AltCustomizableSampleParser = {
        new AltCustomizableSampleParser(newCustomization, path)
      }


      def customize(newPath: String): AltCustomizableSampleParser = {
        new AltCustomizableSampleParser(customization, newPath)
      }
    }


  /** Constructor for a base Parser for Sample files prepared by
   *  Stefano, with no customization.
   *
   *  Samples are assumed to be kept in a sample folder (dbpath).
   *  Each sample is assumed to be kept in two files, one for its
   *  associated bed file (sid), one  for its associated meta data
   *  (sid.meta). And there is a file listing the sid of samples 
   *  to be deserialized.
   *
   *  @param path is the folder where Samples are kept.
   *  @param ois  is an EIterator[String] representing a Sample;
   *              actually, this is just the name of a Sample.
   *  @return     a parsed Sample.
   */

    object AltSampleParser {

      def apply(path: String)(ois: EIterator[String]): Sample = parse(path)(ois)

      def parse(path: String)(ois: EIterator[String]): Sample = {

        val sid = ois.next().trim
        val mf = s"${path}/${sid}.meta"
        val bf = s"${path}/${sid}"

        val metafile = 
          try Source.fromFile(mf)
          catch { case _ : Throwable => throw FileNotFound(mf) }

        val fields = 
          for (l <- metafile.getLines; e = l.trim.split("\t")) 
          yield (e(0) -> e(1))

        try
          Sample(Map(fields.toSeq :_*) + ("sid" -> sid),
                 BedFile.onDiskBedFile(bf))
        finally { metafile.close() } 
      }


      def customize(newCustomization: Sample => Sample):
      AltCustomizableSampleParser = {
        new AltCustomizableSampleParser(newCustomization, "")
      }


      def customize(newPath: String): AltCustomizableSampleParser = {
        new AltCustomizableSampleParser(null, newPath)
      }

    }  // End object AltSampleParser.



  /** Default deserializer for SampleFile, Limsoon's format.
   */

    val deserializerSampleFile: Deserializer[Sample] = {
      EFile.deserializerEFile[Sample](SampleParser)
    }




  /** Customization function for ENCODE Narrow Peak sample.
   *  
   *  @param x is an uncustomized Sample.
   *  @return  a customized Sample.
   *

    def toEncodeNPSampleFile(x: Sample): Sample = x.bedFile.efile match {

      // The Bed file should be on disk or slurped.

      case bf: OnDisk[Bed] => 
        x.bedFileUpdated(BedFile.onDiskEncodeNPBedFile(
          filename = bf.filename,
          nocheck = true))

      case bf: Slurped[Bed] =>
        x.bedFileUpdated(BedFile.slurpedEncodeNPBedFile(bf.raw, bf.filename))

      // It is not on disk or slurped; do nothing.

      case _ => x
    }


    val deserializerEncodeNPSampleFile: Deserializer[Sample] = {
      val parser = SampleParser.customize(toEncodeNPSampleFile _)
      EFile.deserializerEFile[Sample](parser)
    }
   *
   */



  /** Deserializer with auto detect and switch between
   *  Limsoon's and Stefano's formats.
   */

    case class SampleFileDeserializer(
      wls: CustomizableSampleParser, 
      stf: AltCustomizableSampleParser)
    extends Deserializer[Sample] {

      val wlsDeserializer = EFile.deserializerEFile[Sample](wls)
      val stfDeserializer = EFile.deserializerEFile[Sample](stf)

      // stf needs a path to be given. 
      def apply(path: String): SampleFileDeserializer = {
        val stfNew = stf.customize(path)
        new SampleFileDeserializer(wls, stfNew)
      }

    
      // Customize wls and stf in the same way.
      def apply(customization: Sample => Sample): SampleFileDeserializer = {
        val wlsNew = wls.customize(customization)
        val stfNew = stf.customize(customization)
        new SampleFileDeserializer(wlsNew, stfNew)
      }


      // Autoswitch between wls and stf to parse a given file.
      override def parse(filename: String, slurp: Option[String] = None):
      EIterator[Sample] = {
        SampleFileDeserializer.parse(
          wlsDeserializer, stfDeserializer, 
          filename, slurp)
      }

    }  // End class SampleFileDeserializer.



  /** Auto detect and switch between Limsoon's and Stefano's formats.
   *
   *  Detection is based on file extension. 
   */

    object SampleFileDeserializer {

      var suffixtmp = ".sftmp"
      var suffixsav = ".sfsav"

      protected def deserializerChosen(
        f:     String,
        slurp: Option[String],
        wls:   Deserializer[Sample],
        stf:   Deserializer[Sample]):
      Deserializer[Sample] = {

        (f.endsWith(suffixtmp) || f.endsWith(suffixsav), slurp) match {

          // f has suffix for Limsoon's format.
          case (true, _)     => wls 

          // f doesnt have suffix for Limsoon's format. 
          // So assume it is in Stefano's format.
          case (false, None) => stf

          // f is a raw String slurped from the file g. So check g's suffix.
          case (false, Some(g)) =>
            if (g.endsWith(suffixtmp) || g.endsWith(suffixsav)) wls
            else stf
        }
      }


      // Parse SampleFile using the right deserializer.

      def parse(
        wls: Deserializer[Sample],
        stf: Deserializer[Sample],
        filename: String, 
        slurp:    Option[String] = None): 
      EIterator[Sample] = {

        val deserializer = deserializerChosen(filename, slurp, wls, stf)
        deserializer.parse(filename, slurp)

      }

    }  // End object SampleFileDeserializer



  /** Alternative deserializers for SampleFile.
   *  Auto-detect and switch between Limsoon's and Stefano's formats.
   */

    val altDeserializerSampleFile = { 
      val wls = CustomizableSampleParser(null)
      val stf = AltCustomizableSampleParser(null, "")
      SampleFileDeserializer(wls, stf)
    }


  /** Customization for ENCODE Narrow Peak.
   *

    val altDeserializerEncodeNPSampleFile = {
      altDeserializerSampleFile(toEncodeNPSampleFile _)
    }
  *
  */


  }  // End object SampleFileSerializers 





  /**
   * Set up customized EIterator and EFile for Sample.
   * Needed because SampleFile has its special formats on disk.
   */


  type SampleEIterator = EIterator[Sample]

  type SampleFile = EFile[Sample]

  type SampleFileSettings = EFileSettings[Sample]


  object SampleFile {

    import SampleFileSerializers._



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

    def makeSettingsSampleFile(
      deserializer: Deserializer[Sample] = deserializerSampleFile):
    EFileSettings[Sample] = {

      EFile.setDefaultsEFile[Sample](
        suffixtmp       = ".sftmp",
        suffixsav       = ".sfsav",
        aveSz           = 1000,
        cap             = 70000, 
        totalsizeOnDisk = totalsizeOnDiskSampleFile _,
        serializer      = serializerSampleFile,
        deserializer    = deserializer)
    }

  
    val defaultSettingsSampleFile = makeSettingsSampleFile()


    def altSettingsSampleFile(path: String = "") = {
      val deserializer = altDeserializerSampleFile(path)
      makeSettingsSampleFile(deserializer)
    }


  /** Customization for ENCODE Narrow Peak.
   *

    val encodeNPSettingsSampleFile =
      makeSettingsSampleFile(deserializerEncodeNPSampleFile)

    def altSettingsEncodeNPSampleFile(path: String = "") = {
      val deserializer = altDeserializerEncodeNPSampleFile(path)
      makeSettingsSampleFile(deserializer)
    }

  *
  */



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

        inMemoryEFile[Sample](entries, settings)

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

      transientEFile[Sample](entries, settings)

    }

    

  /** Construct on-disk SampleFile.
   *
   *  @param filename is name of the SampleFile.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */
  
    def onDiskSampleFile(
      filename: String,
      settings: EFileSettings[Sample] = defaultSettingsSampleFile,
      nocheck : Boolean = false)
    : SampleFile = {

      onDiskEFile[Sample](filename, settings, nocheck)
    
    }


  /** Customization for ENCODE Narrow Peak.
   *

    def onDiskEncodeNPSampleFile(
      filename: String,
      settings: EFileSettings[Sample] = encodeNPSettingsSampleFile,
      nocheck : Boolean = false)
    : SampleFile = {

      onDiskEFile[Sample](filename, settings, nocheck)
    }

  *
  */



  /** Construct an on-disk SampleFile from files created by Stefano Perna.
   *
   *  @param dbpath is a folder of Samples.
   *  @param samplelist is list of Samples to deserialize.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.
   */

    def altOnDiskSampleFile
      (dbpath: String) 
      (samplelist: String,
       settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
    : SampleFile = {

      onDiskEFile[Sample](samplelist, settings)

    }

    

  /** Construct an on-disk SampleFile from files created by Stefano Perna,
   *  customize their Bed entries as ENCODE Narrow Peaks. 
   *
   *  @param dbpath is a folder of Samples.
   *  @param samplelist is list of Samples to deserialize.
   *  @param settings is the settings to use.
   *  @return the constructed SampleFile.

    def altOnDiskEncodeNPSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsEncodeNPSampleFile(dbpath))
    : SampleFile = {

      onDiskEFile[Sample](samplelist, settings)

    }
  *
  */



  /** @param efobj is a SampleFile.
   *  @return the total size of efobj if it is on disk,
   *     inclusive of all its component files.
   */
 
    def totalsizeOnDiskSampleFile(efobj: SampleFile):Double = 
      efobj.efile match {

        case ef: Transient[Sample] => 0

        case ef: Slurped[Sample] => totalsizeOnDiskSampleFile(efobj.slurped)

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



