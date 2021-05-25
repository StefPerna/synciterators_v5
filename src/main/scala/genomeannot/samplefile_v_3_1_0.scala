

package synchrony.gmql

/** Set up customized EIterator and EFile for Sample.
 *  Needed because SampleFile has its special formats on disk.
 *
 * Wong Limsoon
 * 15 April 2021
 */


object SampleFiles {

  import scala.io.Source
  import java.nio.file.{ Files, Paths }
  import scala.reflect.classTag
  import synchrony.iterators.FileCollections._
  import EFile._
  import synchrony.iterators.Serializers._
  import synchrony.genomeannot.SplitFiles.{ SBedSerializers, SBedFile }
  import synchrony.gmql.Samples.{ Bed, BedFile, Sample, BaseSampleFileSerializers }


  type SampleFile         = EFile[Sample]
  type SampleEIterator    = EIterator[Sample]
  type SampleFileSettings = EFileSettings[Sample]



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

    val Base = BaseSampleFileSerializers
    val SampleFormatter = Base.SampleFormatter
    val serializerSampleFile = Base.serializerSampleFile


  /** Constructor for a base Parser for Sample, with no customization,
   *  in Limsoon's format.
   *
   */

    object SampleParser
    extends Base.SampleParser(sid => 
      SBedSerializers.SBedFileDeserializer.isBedFile(sid) match {
        case true  => BedFile.onDiskBedFile(sid)
        case false => SBedFile.onDiskSBedFile(sid)
      }
    )
    


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
          for (l <- metafile.getLines(); e = l.trim.split("\t")) 
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



  /** Deserializer with auto detect and switch between
   *  Limsoon's and Stefano's formats.
   */

    case class SampleFileDeserializer(
      wls: Base.CustomizableSampleParser, 
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
      val wls = Base.CustomizableSampleParser(SampleParser, null)
      val stf = AltCustomizableSampleParser(null, "")
      SampleFileDeserializer(wls, stf)
    }

  }  // End object SampleFileSerializers 



  case class ASampleFile(override val efile: EFileState[Sample])
  extends SampleFile


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
      deserializer: Deserializer[Sample] = deserializerSampleFile,
      serializer: Serializer[Sample] = serializerSampleFile):
    EFileSettings[Sample] = {

      EFile.setDefaultsEFile[Sample](
        suffixtmp       = ".sftmp",
        suffixsav       = ".sfsav",
        aveSz           = 1000,
        cap             = 70000, 
        totalsizeOnDisk = totalsizeOnDiskSampleFile _,
        serializer      = serializer,
        deserializer    = deserializer
       )(classTag[Sample], Sample.dummyOrder)
    }

  
    val defaultSettingsSampleFile = makeSettingsSampleFile()


    def altSettingsSampleFile(path: String = "") = {
      val deserializer = altDeserializerSampleFile(path)
      makeSettingsSampleFile(deserializer)
    }



  /** Construct SampleFile from EFileState 
   *
   *  @param efile is an EFileState.
   *  @return the constructed SampleFile.
   */
   
    def apply(efile: EFileState[Sample]): SampleFile = new ASampleFile(efile)


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

        inMemoryEFile[Sample](entries)(settings)

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

      transientEFile[Sample](entries)(settings)

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
      onDiskEFile[Sample](filename, nocheck)(settings)
    }


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
      onDiskEFile[Sample](samplelist)(settings)
    }

    
    def importSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsSampleFile(dbpath))
      (importAs: String, importTo: String = "")
    : SampleFile = {
      def nameOf(s: Sample) = s"${importAs}-${s("sid")}"
      val np = altOnDiskSampleFile(dbpath)(samplelist, settings)
      val ns = for (
                 u <- np.eiterator;
                 s = if (u.bedFile.isSorted) u.trackUpdated(u.track)
                     else u.trackSorted
               ) yield s.trackSavedAs(nameOf(s), importTo)
      SampleFile.transientSampleFile(ns).savedAs(importAs, importTo)
    }

    

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
            _.flatAggregateBy(sum[Sample](_.bedFile.totalsizeOnDisk.toDouble))
          )
        }

        case ef: OnDisk[Sample] => {
          import synchrony.iterators.FileCollections.implicits._
          import synchrony.iterators.AggrCollections.OpG.sum
          efobj.filesize +
          efobj.processedWith[Double](
            _.flatAggregateBy(sum[Sample](_.bedFile.totalsizeOnDisk.toDouble))
          )
        }
      }


    
  /** Transform a SampleFile's BEDFiles into SBedFiles.
   *  I.e. split by BED entries of each sample by chromosomes.
   *
   *  @param samples  - the SampleFile
   *  @param importAs - new file name
   *  @param importTo - directory to put the new file in
   *  @return the transformed SampleFile
   */

    def importSplitByChrom(
      samples: SampleFile,
      importAs: String = "",
      importTo: String = ""
    ) : SampleFile = {

      if (importTo != "") Files.createDirectories(Paths.get(importTo))
      
      val pfix = samples.efile.settings.prefix

      val name = (importTo, importAs) match{
        case ("", "") => Files.createTempFile(
          pfix,
          SampleFile.defaultSettingsSampleFile.suffixsav).toString
        case ("", f)  => f
        case (d, "")  => Files.createTempFile(
          Paths.get(d), 
          pfix,
          SampleFile.defaultSettingsSampleFile.suffixsav).toString
        case (d, f)   => d + "/" + f
      }

      val sf = SampleFile.transientSampleFile{
        for (
          s <- samples.eiterator;
          b = SBedFile(s.bedFile, "", importTo)
        ) yield s.bedFileUpdated(b)
      }.savedAs(name)

      return onDiskSampleFile(sf.filename)
    }

  }  // End object SampleFile


  // Put a copy of FileCollections.implicits here for convenience.

  val implicits = synchrony.iterators.FileCollections.implicits

}


