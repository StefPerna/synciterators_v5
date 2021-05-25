
package synchrony.gmql

/** ENCODE develops its own variants of Bed formats that interpret
 *  the optional Bed fields in different ways. This module shows
 *  an example (ENCODE Narrow Peak) on how one can easily support
 *  these subformats.
 * 
 * Wong Limsoon
 * 8/10/2020
 */


object EncodeNP {


  import synchrony.iterators.Serializers._
  import synchrony.iterators.FileCollections._
  import synchrony.genomeannot.BedWrapper.BedSerializers
  import synchrony.gmql.Samples._




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

    private val translate = Map("6" -> "signalval",
                                "7" -> "pval",
                                "8" -> "qval",
                                "9" -> "peak")

    private val minus = Vector("6", "7", "8", "9")

    private def optField(field: Any): Double = field match {
      case x: Double => x
      case x: String => try x.toDouble catch { case _: Throwable => -1.0 }
      case _ => -1
    }
      
    private def encodeMisc(misc: Map[String, Any]) = {
      val plus = for (k <- minus;
                      v = misc.get(k) match {
                            case None => -1
                            case Some(u) => optField(u) })
                 yield (translate(k) -> v)
      misc -- minus ++ plus 
    }


    def toEncodeNPBedFile(bed: Bed): Bed = { 
      bed.hasMisc("peak") match {
        case true  => bed 
        case false => bed.transformMisc(encodeMisc)
      }
    }



    val deserializerEncodeNPBedFile: Deserializer[Bed] = {
      val parser = BedSerializers.CustomizableBedParser(toEncodeNPBedFile _)
      new FileDeserializerWithPosition[Bed](
            parser.parseRaw _,
            parser,
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

      EFile.onDiskEFile[Bed](filename, settings, nocheck)

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

      EFile.onDiskEFile[Sample](filename, settings, nocheck)

    }



    def altOnDiskEncodeNPSampleFile
      (dbpath: String)
      (samplelist:String,
       settings: EFileSettings[Sample] = altSettingsEncodeNPSampleFile(dbpath))
    : SampleFile = {

      EFile.onDiskEFile[Sample](samplelist, settings)

    }

}  // End object EncodeNP


 
