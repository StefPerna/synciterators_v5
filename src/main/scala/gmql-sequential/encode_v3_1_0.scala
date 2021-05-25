
package synchrony.gmql

/** ENCODE develops its own variants of Bed formats that interpret
 *  the optional Bed fields in different ways. This module shows
 *  an example (ENCODE Narrow Peak) on how one can easily support
 *  these subformats.
 * 
 * Wong Limsoon
 * 18/4/2021
 */


object EncodeNP {

  import synchrony.iterators.Serializers._
  import synchrony.iterators.FileCollections._
  import synchrony.genomeannot.BedWrapper.BedSerializers
  import synchrony.genomeannot.SplitFiles.SBedFile
  import synchrony.gmql.Samples._
  import synchrony.gmql.SampleFiles._



  //
  // Customizing Bed files for ENCODE Narrow Peak subformat.
  //



  /** Customization for extracting ENCODE narrow peak attributes
   *  from the misc: Map[String,Any] of a Bed entry. Encode Narrow Peak
   *  files have the standard 6 Bed fields, followed by 4 ENCODE narrow
   *  peak-specific fields.
   *
   *  @param  bed is the Bed entry that needs customization.
   *  @return     the customized Bed entry. Basically, fields "6" to
   *             "9" get renamed to their ENCODE narrow peak name.
   */

    private val translate = Array("6" -> "signalval",
                                  "7" -> "pval",
                                  "8" -> "qval",
                                  "9" -> "peak")

    private val minus = Array("6", "7", "8", "9")

    private def encodeMisc(misc: Map[String, Any]) = {
      val plus = for ((k, f) <- translate; v = misc.getOrElse(k, -1))
                 yield (f -> v)
      misc -- minus ++ plus 
    }


    def toEncodeNPBedFile(bed: Bed): Bed = { 
      bed.hasMisc("peak") match {
        case true  => bed 
        case false => bed.transformMisc(encodeMisc)
      }
    }


    val deserializerEncodeNPBedFile: Deserializer[Bed] = {
      new FileDeserializerWithPosition[Bed](
        new BedSerializers.CustomizableBedParser(toEncodeNPBedFile _),
        (e: String) => !(e startsWith "track"))
    }


    val encodeNPSettingsBedFile = EFile.setDefaultsEFile[Bed](
      suffixtmp       = ".bed",
      suffixsav       = ".bed",
      aveSz           = 1000,
      cap             = 100000,  
      totalsizeOnDisk = BedFile.totalsizeOnDiskBedFile _,
      serializer      = BedSerializers.serializerBedFile,
      deserializer    = deserializerEncodeNPBedFile
    ) 
   


  /** Construct an on-disk ENCODE Narrow Peak BedFile.
   *
   *  @param filename is name of the input file.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def onDiskEncodeNPBedFile(
      filename: String,
      settings: EFileSettings[Bed] = encodeNPSettingsBedFile,
      nocheck: Boolean = false): 
    BedFile = {

      EFile.onDiskEFile[Bed](filename, nocheck)(settings)

    }



  /** Construct a slurped ENCODE Narrow Peak BedFile.
   *
   *  @param raw is String encoding the BedFile.
   *  @param filename is name of the file encoded by raw.
   *  @param settings is the settings of the BedFile.
   *  @return the constructed BedFile.
   */

    def slurpedEncodeNPBedFile(
      raw: String,
      filename: String,
      settings: EFileSettings[Bed] = BedFile.defaultSettingsBedFile): 
    BedFile = {

      BedFile.slurpedBedFile(raw, filename, settings)

    }



  //
  // Customizing SampleFile for ENCODE Narrow Peak.
  //



  /** Customization for Sample that has ENCODE Narrow Peak as its track.
   *
   *  @param x is an uncustomized Sample.
   *  @return  a customized Sample.
   */


    def toEncodeNPSampleFile(x: Sample): Sample = x.bedFile.efile match {

      // The Bed file should be on disk or slurped.
      case bf: OnDisk[Bed] => 
        x.bedFileUpdated(onDiskEncodeNPBedFile(
          filename = bf.filename,
          nocheck = true))

      case bf: Slurped[Bed] =>
        x.bedFileUpdated(slurpedEncodeNPBedFile(bf.raw, bf.filename))

      // It is not on disk or slurped; do nothing.
      case _ => x
    }


    val deserializerEncodeNPSampleFile: Deserializer[Sample] = {
      val parser = SampleFileSerializers
                   .SampleParser
                   .customize(toEncodeNPSampleFile _)
      EFile.deserializerEFile[Sample](parser)
    }


    val altDeserializerEncodeNPSampleFile = {
      SampleFileSerializers.altDeserializerSampleFile(toEncodeNPSampleFile _)
    }


    val encodeNPSettingsSampleFile =
      SampleFile.makeSettingsSampleFile(deserializerEncodeNPSampleFile)

    def altSettingsEncodeNPSampleFile(path: String = "") = {
      val deserializer = altDeserializerEncodeNPSampleFile(path)
      SampleFile.makeSettingsSampleFile(deserializer)
    }



    def onDiskEncodeNPSampleFile(
      filename: String,
      settings: EFileSettings[Sample] = encodeNPSettingsSampleFile,
      nocheck : Boolean = false)
    : SampleFile = {
      EFile.onDiskEFile[Sample](filename, nocheck)(settings)
    }



    def altOnDiskEncodeNPSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsEncodeNPSampleFile(dbpath))
    : SampleFile = {
      EFile.onDiskEFile[Sample](samplelist)(settings)
    }



    def importEncodeNPSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsEncodeNPSampleFile(dbpath))
      (importAs: String, importTo: String = "")
    : SampleFile = {
      def nameOf(s: Sample) = s"${importAs}-${s("sid")}"
      val np = altOnDiskEncodeNPSampleFile(dbpath)(samplelist, settings)
      val ns = for (
                 u <- np.eiterator;
                 s = if (u.bedFile.isSorted) u.trackUpdated(u.track)
                     else u.trackSorted
               ) yield s.trackSavedAs(nameOf(s))
      SampleFile.transientSampleFile(ns).savedAs(importAs, importTo)
    }


    def importEncodeNPSplitByChrom
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsEncodeNPSampleFile(dbpath))
      (importAs: String, importTo: String = "")
    : SampleFile = {
      val np = altOnDiskEncodeNPSampleFile(dbpath)(samplelist, settings)
      val ns = for (
                 u <- np.eiterator;
                 b = if (u.bedFile.isSorted) u.bedFile else u.bedFile.sorted;
                 s = SBedFile(b, u.getM[String]("sid"), importTo)
               ) yield u.bedFileUpdated(s)
      SampleFile.transientSampleFile(ns).savedAs(importAs, importTo)
    }


}  // End object EncodeNP


 
