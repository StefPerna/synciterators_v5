

package gmql

/** Version 5, to be used with gmql.BEDModel Version 5.
 *
 *  Wong Limsoon
 *  30 Jan 2022
 */


object SAMPLEModel {

/** Organization:
 *
 *  object SAMPLE
 *    // Contains: Sample
 *
 *  object SAMPLEFile = gmql.SAMPLEFile
 *    // Contains: SampleFile, OCollToSampleFile
 */

  import scala.language.implicitConversions
  import java.nio.file.{ Files, Paths }
  import gmql.BEDModel.BEDEntry.Bed
  import gmql.BEDModel.BEDFile.{ BedFile, BEDFILE, BEDITERATOR }
//  import dbmodel.DBModel.CloseableIterator

  type Bool    = Boolean
  type META    = Map[String,Any]  // Sample meta data
  type SID     = String           // Sample key


  def TMP = dbmodel.DBFile.TMP    // Folder for temporary files



  object SAMPLE {

  /** A [[Sample]], modelled after GMQL, comprises a BED file and
   *  its associated meta data.  A BED file is represented as
   *  [[gmql.BEDModel.BEDEntry.Bed]].  Its meta data is basically
   *  a list of key-value pairs of heterogeneous types; so, it is 
   *  represented as [[Map[String,Any]].
   */



    case class Sample(meta: META, bedFile: BEDFILE)
    {

      /** [[track]] is a [[FileIterator]] on [[bedFile]].
       *  This lets you iterate on [[bedFile]] many times.
       */

      def track: BEDITERATOR = bedFile.cbi


      /** trackSz is size of [[bedFile]].
       */

      def trackSz: Long = 
        try Files.size(Paths.get(bedFile.filename))
        catch { case _: Throwable => 0 }


      /** Check whether a list of predicates all hold on this Sample.
       */
  
      def cond(ps: (Sample, Bed) => Boolean*)(u: Bed): Boolean =
        ps.forall(p => p(this, u))

      def cond(ps: Sample => Boolean*): Boolean = ps.forall(p => p(this))


      /** Check whether a field is present in this Sample's meta data.
       */

      def apply(f: String): Any = meta(f)

      def getInt(f: String): Int = getMeta[Int](f)

      def getDbl(f: String): Double = getMeta[Double](f)

      def getStr(f: String): String = getMeta[String](f)

      def getMeta[A](f: String): A = meta(f).asInstanceOf[A]

      def hasMeta(f: String): Boolean = meta.contains(f)

      def checkMeta[A](
        f:     String, 
        check: A => Boolean, 
        error: Bool = false): Bool= 
      {
        try check(meta(f).asInstanceOf[A]) catch { case _: Throwable => error }
      }


      def sid: String = meta.getOrElse("sid", "no sid").asInstanceOf[String]


      /** Methods for updating the meta data of this Sample.
       */

      def +(fv: (String,Any)): Sample    = Sample(meta + fv, bedFile)

      def ++(fvs: (String,Any)*): Sample = Sample(meta ++ fvs, bedFile)

      def ++(m: META): Sample            = Sample(meta ++ m, bedFile)

      def newMeta(m: META): Sample       = Sample(m, bedFile)

      def -(f: String): Sample           = Sample(meta - f, bedFile)

      def delMeta(): Sample            = Sample(meta.empty, bedFile)


      /** Conservative merging of meta data. If there are conflicts,
       *  dont overwrite current values.
       */

      def mergeMeta(m: META): Sample = Sample(m ++ meta, bedFile)


      /** Rename meta data of this Sample.
       */

      def renameMeta(r: String => String): Sample = 
      {
        val renamed = for ((f, v) <- meta) yield r(f) -> v
        Sample(renamed, bedFile)
      }


     /** Methods to update the BED file of this Sample.
      */

      def newTrack(bf: BEDFILE): Sample = Sample(meta, bf)

      def transientTrack(it: BEDITERATOR): Sample = 
        Sample.transientSample(meta, it)
  
      def mergeTrack(bf: BEDFILE): Sample = newTrack(bedFile.mergedWith(bf))

      def mergeTrack(it: BEDITERATOR): Sample = {
        val transient = BedFile.transientBedFile(it)
        val merged    = bedFile.mergedWith(transient)
        newTrack(merged)
      }

      def delTrackMeta(): Sample = newTrack(bedFile.map(_.delMeta()))
 

      /** [[track]] is normally already sorted on the locus of BED entries.
       *  But sometimes it is not sorted. So, provide a sort function
       *  to get it in order.
       */

      def orderedTrack(implicit bufSz: Int = 10000): Sample =
        newTrack(bedFile.orderedBy(bedFile.key, bufSz))


      /** Copy or rename [[track]] to a new file.
       */

      def saveTrackAs(name: String, folder: String = TMP): Sample = 
      {
        Sample(meta, bedFile.saveAs(name, folder))
      }

      def renameTrackAs(name: String, folder: String = TMP): Sample =
        Sample(meta, bedFile.renameAs(name, folder))

      def trackSerialized(
        implicit name: String = "",
               folder: String = TMP): Sample =
      {
        Sample(meta, bedFile.serialized(name, folder))
      }

    }  // End class Sample



    object Sample {
  
      /** Alternative constructors for Sample.
       */

    
      def apply(meta: META, filename: String): Sample = 
        Sample(meta, filename, TMP)

      def apply(meta: META, filename: String, folder: String): Sample =
        Sample(meta, BedFile(filename, folder))


      def encodeSample(meta: META, filename: String): Sample =
        encodeSample(meta, filename, TMP)

      def encodeSample(meta: META, filename: String, folder: String): Sample =
        Sample(meta, BedFile.encode(filename, folder))


      def transientSample(meta: META, it: BEDITERATOR): Sample =
        Sample(meta, BedFile.transientBedFile(it))

    }  // End object Sample

  }  // End object SAMPLE




  object SAMPLEFile {

    import scala.language.implicitConversions
    import dbmodel.DBModel.CloseableIterator.CBI
    import dbmodel.DBModel.OrderedCollection.{ Key, OColl }
    import dbmodel.DBFile.{ 
      OFile, ODisk, OTransient, OMemory, OCollToFile, 
      PARSER, FORMATTER, DESTRUCTOR }
    import SAMPLE.Sample 
    import BEDModel.BEDFile.{ BEDFILE, BedFile }


    type Bool           = Boolean
    type SID            = String
    type SAMPLEFILE     = OFile[Sample,SID]
    type SAMPLEITERATOR = CBI[Sample]



    implicit class OCollToSampleFile(entries: OColl[Sample,SID]) 
    {
      def toSampleFile: SAMPLEFILE = SampleFile.transientSampleFile(entries.cbi)
    }



    object SampleFile
    {
      /** Key for SampleFile
       */

      val sampleFileKey: Key[Sample,SID] =
        Key(_.getMeta[SID]("sid"), Ordering[SID])


      /** Constructors for SampleFile
       */

      def gmqlSampleFile(path: String, filename: String): SAMPLEFILE =
        gmqlSampleFile(path, filename, TMP)


      def gmqlSampleFile(
        path:     String, 
        filename: String, 
        folder:   String): SAMPLEFILE = 
      {
        // Assume filename is Sample file in GMQL format.
        val fname = OFile.mkFileName(filename, folder).toString
        val gmql  = OFile(sampleFileKey, fname, parserGMQL(path), formatter)
        OMemory(sampleFileKey, gmql.elemSeq, 
                parser, formatter, destructorSampleFile _)
      }


      def gmqlEncodeSampleFile(path: String, filename: String): SAMPLEFILE =
        gmqlEncodeSampleFile(path, filename, TMP)


      def gmqlEncodeSampleFile(
        path:     String, 
        filename: String,
        folder:   String): SAMPLEFILE =
      {
        // Assume filename is Sample file in GMQL format.
        val fname = OFile.mkFileName(filename, folder).toString
        val gmql  = OFile(sampleFileKey, 
                          fname, 
                          parserGMQL(path, BedFile.encode _), 
                          formatter)
        OMemory(sampleFileKey, gmql.elemSeq,
                parser(BedFile.encode _),formatter, destructorSampleFile _)
      }
      

      def encode(filename: String): SAMPLEFILE = encode(filename, TMP)
  

      def encode(filename: String, folder: String): SAMPLEFILE = 
      {
        // Assume filename is a Sample file in Limsoon's format.
        val fname = OFile.mkFileName(filename, folder).toString
        if (Files.exists(Paths.get(fname))) {
          ODisk(sampleFileKey, fname,
                parser(BedFile.encode _), formatter, destructorSampleFile _)
        }
        else throw new java.io.FileNotFoundException(filename)
      }


      def apply(filename: String): SAMPLEFILE = SampleFile(filename, TMP)


      def apply(filename: String, folder: String): SAMPLEFILE = 
      {
        // Assume filename is a Sample file in Limsoon's format.
        val fname = OFile.mkFileName(filename, folder).toString
        if (Files.exists(Paths.get(fname))) {
          ODisk(sampleFileKey, fname, parser, formatter, destructorSampleFile _)
        }
        else throw new java.io.FileNotFoundException(filename)
      }


      /** Delete a Sample file. Provide a deep-delete option.
       */

      def destructorSampleFile(sf: OFile[Sample,_]): Unit =
      {
        // Assume that if sf is not protected then all its BED files,
        // if their name is prefixed by sf's name, should be deleted
        // along with sf.
        if (!sf.protect && sf.filename != "") { 
          sf.foreach { s => 
            (s.bedFile.filename startsWith sf.filename) match {
              case true  => s.bedFile.protection(false).close()
              case false => s.bedFile.close()
            }
          }
        }

        if (!sf.protect && sf.filename == "") sf.foreach { _.bedFile.close() }

        // Now can delete sf itself.
          
        OFile.destructorOFile(sf)
      }

    
      /** Copy a sample file. Provide a deep-copy option.
       */

      def deepCopy
        (sf: SAMPLEFILE, filename: String, folder: String = TMP)
        (implicit deep: Boolean = true): SAMPLEFILE =
      {
        if (deep) sf.ocbi.saveAs(filename, folder)
        else sf.saveAs(filename, folder) 
      }



      /** Sometimes, it is handy to have a transient Sample file for
       *  temporary use without writing it out to disk. Since Sample
       *  files are usually small, just keep them materialized.
       */
  
      def transientSampleFile(entries: IterableOnce[Sample]): SAMPLEFILE = {
        OTransient[Sample,SID](
          sampleFileKey, CBI(entries), 
          parser, formatter, 
          destructorSampleFile _).materialized
      }


      /** Formatter for Sample file.
       */

      private  val header = "##wonglimsoon@nus##"
  
      val formatter: FORMATTER[Sample] = (filename: String) =>
      {
        def format(s: Sample, position: Int = 1): String =
        {
          val dir    = Paths.get(filename).getParent()
          val par   = filename.split('\\').last
          val prefix = s"${par}-ofile"
          val fname = Files.createTempFile(dir, prefix, ".bed").toString

          // The track of Sample s must be serialized first, so that the
          // track is represented by its filename. This filename is added
          // to Sample's meta data as its sid field.

          val sid = s.bedFile.serialized(name = fname, folder = TMP).filename
          val fvs = s.meta.map { case (f, v) => s"$f\t$v\n" }
  
          // Include header if it is the first Sample
          if (position == 0) s"$header\n$sid\n${fvs.mkString}"
          else s"$sid\n${fvs.mkString}"
        }

        format _
      }


      /** Parser for Sample file.  Need to support the self-describing
       *  format of Limsoon's Sample file, and also the rather inconvenient
       *  format of GMQL Sample file. The GMQL format is terribly messy,
       *  things are not kept in one file and needs extra information
       *  (a path) to access... Arrggghhh!
       */


      def parseWLS
        (mkBedFile: String => BEDFILE = BedFile.apply _)
        (ois: Iterator[String], position: Int = 1): Sample = 
      {
        // 1st line is header. Skip it.
        if (position == 0) ois.next()
  
        // Parse a Sample file in Limsoon's format.
        // The first non-blank line is path to the Sample's Bed file.
        var sid = ois.next().trim
        while (sid == "") { sid = ois.next().trim }
        // All following lines, until the next blank line, are
        // the meta attributes of this Sample.
        val lines = ois.takeWhile(_.trim != "").toVector
        val fvs = for(l <- lines;
                      fv = l.trim.split("\t");
                      f: String = fv(0);
                      v: Any = try fv(1).toInt    catch { case _: Throwable =>
                               try fv(1).toDouble catch { case _: Throwable =>
                               fv(1) } } ) 
                  yield (f -> v)
        val meta = Map[String, Any](fvs.toSeq :_*)
        Sample(meta, mkBedFile(sid)) 
      }


      def parseGMQL
        (path: String, mkBedFile: String => BEDFILE = BedFile.apply)
        (ois: Iterator[String], position: Int = 1): Sample =
      {
        val sid = ois.next().trim
        val mf = s"${path}/${sid}.meta"
        val bf = s"${path}/${sid}"
        val metafile = scala.io.Source.fromFile(mf)
        val lines = metafile.getLines().toSeq
        val fvs = for (l <- lines;
                       fv = l.trim.split("\t");
                       f: String  = fv(0);
                       v: Any = try fv(1).toInt    catch { case _: Throwable =>
                                try fv(1).toDouble catch { case _: Throwable =>
                                fv(1) } } ) 
                  yield (f -> v)
        try Sample(Map(fvs.toSeq :_*) + ("sid" -> sid), mkBedFile(bf))
        finally { metafile.close() } 
      }


      def parser(
        implicit
          mkBedFile: String => BEDFILE = BedFile.apply _): PARSER[Sample] =
      {
        (filename: String) => parseWLS(mkBedFile)
      }


      def parserGMQL(
        implicit
          path: String = "",
          mkBedFile: String => BEDFILE = BedFile.apply _) : PARSER[Sample] =
      {
        (filename: String) => {  
          if (filename == null) parseWLS(mkBedFile)
          else {
            val file = scala.io.Source.fromFile(filename)
            try {
              val line = file.getLines().find(_ => true)
              line match {
                case Some(l) => if (l.trim == header) parseWLS(mkBedFile)
                              else parseGMQL(path, mkBedFile)
                case _ => parseGMQL(path, mkBedFile) } }
            finally file.close()
          }
        }
      }  // End def parser

    }  // End object SampleFile  

  }  // End object SAMPLEFile


}  // End object SampleModel


